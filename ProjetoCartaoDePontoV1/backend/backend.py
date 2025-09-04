# -*- coding: utf-8 -*-
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import re
from datetime import datetime, timedelta, date, time
from openpyxl.styles import Font
from typing import List

# --- CONFIGURAÇÃO E CONSTANTES GLOBAIS ---
app = FastAPI(title="API Conversora de Ponto")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["*"],
)

JORNADA_SEMANAL = timedelta(hours=8)
JORNADA_SABADO = timedelta(hours=4)
FERIADOS_2025 = {
    date(2025, 1, 1), date(2025, 3, 4), date(2025, 4, 18), date(2025, 4, 21),
    date(2025, 5, 1), date(2025, 6, 19), date(2025, 7, 9), date(2025, 9, 7), 
    date(2025, 10, 12), date(2025, 11, 2), date(2025, 11, 15), 
    date(2025, 11, 20), date(2025, 12, 25),
}
DIAS_SEMANA = {
    0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira',
    4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'
}

# --- FUNÇÕES ---

def processar_multiplos_arquivos(arquivos: List[UploadFile]):
    """Orquestra todo o processo: parse, detecção de faltas, cálculo e geração do Excel."""

    # 1. Parse de TODOS os arquivos para um único DataFrame
    dados_consolidados = []
    for arquivo in arquivos:
        conteudo = arquivo.file.read().decode("utf-8")
        arquivo.file.seek(0)
        padrao_data_hora = re.compile(r'(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2}:\d{2})')
        for linha in conteudo.splitlines():
            match = padrao_data_hora.search(linha)
            if match:
                try:
                    data_str, hora_str = match.groups()
                    info_inicial = linha[:match.start()].split()
                    nome = info_inicial[1] if len(info_inicial) > 1 else 'N/A'
                    dados_consolidados.append({
                        "nome": nome,
                        "data": datetime.strptime(data_str, "%d.%m.%Y").date(),
                        "hora": datetime.strptime(hora_str, "%H:%M:%S").time()
                    })
                except (ValueError, IndexError):
                    continue

    if not dados_consolidados:
        raise ValueError("Nenhum dado válido foi encontrado nos arquivos enviados.")

    df_raw = pd.DataFrame(dados_consolidados)
    df_raw['data'] = pd.to_datetime(df_raw['data'])

    # 2. Lógica de Detecção de Faltas e Cálculo de Horas
    relatorio_diario = []
    min_date = df_raw['data'].min().date()
    max_date = df_raw['data'].max().date()
    periodo_completo = pd.date_range(start=min_date, end=max_date)
    todos_funcionarios = df_raw['nome'].unique()

    for funcionario in todos_funcionarios:
        df_funcionario_raw = df_raw[df_raw['nome'] == funcionario]
        
        for data_atual in periodo_completo:
            data_atual_obj = data_atual.date()
            dia_semana_num = data_atual_obj.weekday()
            
            grupo = df_funcionario_raw[df_funcionario_raw['data'].dt.date == data_atual_obj]
            
            normais, a_dever, extras_comuns, extras_100 = timedelta(), timedelta(), timedelta(), timedelta()
            total_trabalhado = timedelta(0)

            if not grupo.empty: # Dia foi trabalhado
                horarios = sorted([datetime.combine(data_atual_obj, h) for h in grupo["hora"]])
                
                if len(horarios) == 2:
                    entrada, saida = horarios[0], horarios[1]
                    almoco_inicio = datetime.combine(data_atual_obj, time(12,0))
                    almoco_fim = datetime.combine(data_atual_obj, time(14,0))
                    if entrada < almoco_inicio and saida > almoco_fim:
                        horarios = [entrada, almoco_inicio, almoco_fim, saida]
                
                total_trabalhado = sum([horarios[i+1] - horarios[i] for i in range(0, len(horarios) - 1, 2)], timedelta())
            
                if dia_semana_num == 6 or data_atual_obj in FERIADOS_2025:
                    extras_100 = total_trabalhado
                elif dia_semana_num == 5:
                    normais = min(total_trabalhado, JORNADA_SABADO)
                    if total_trabalhado > JORNADA_SABADO: extras_comuns = total_trabalhado - JORNADA_SABADO
                    else: a_dever = JORNADA_SABADO - total_trabalhado
                else:
                    normais = min(total_trabalhado, JORNADA_SEMANAL)
                    if total_trabalhado > JORNADA_SEMANAL: extras_comuns = total_trabalhado - JORNADA_SEMANAL
                    else: a_dever = JORNADA_SEMANAL - total_trabalhado
            
            else: # Dia não foi trabalhado
                if dia_semana_num == 6 or data_atual_obj in FERIADOS_2025:
                    pass # É folga, não faz nada
                elif dia_semana_num == 5:
                    a_dever = JORNADA_SABADO
                else:
                    a_dever = JORNADA_SEMANAL
            
            if total_trabalhado > timedelta(0) or a_dever > timedelta(0):
                 relatorio_diario.append({
                    "Data": data_atual_obj, "Funcionário": funcionario, "Dia da Semana": DIAS_SEMANA.get(dia_semana_num, ''),
                    "Total Trabalhado": total_trabalhado, "Horas Normais": normais,
                    "Horas a Dever": a_dever, "Horas Extras (Comum)": extras_comuns,
                    "Horas Extras (100%)": extras_100,
                })

    df_calculado = pd.DataFrame(relatorio_diario)

    # 3. Geração do Relatório Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_calculado.sort_values(by=['Funcionário', 'Data'], inplace=True)
        for funcionario in df_calculado["Funcionário"].unique():
            df_funcionario = df_calculado[df_calculado["Funcionário"] == funcionario].copy()

            # CORREÇÃO: A formatação da data é feita de forma segura com .apply()
            df_funcionario['Data'] = df_funcionario['Data'].apply(lambda d: d.strftime('%d/%m/%Y'))
            
            df_funcionario.to_excel(writer, index=False, sheet_name=funcionario)

            worksheet = writer.sheets[funcionario]
            time_format = '[h]:mm:ss'
            
            for col_idx, col_name in enumerate(df_funcionario.columns, 1):
                if "Hora" in col_name or "Trabalhado" in col_name:
                    for row_idx in range(2, len(df_funcionario) + 2):
                        worksheet.cell(row=row_idx, column=col_idx).number_format = time_format

            totals = {
                "Normais": df_calculado[df_calculado["Funcionário"] == funcionario]["Horas Normais"].sum(),
                "A Dever": df_calculado[df_calculado["Funcionário"] == funcionario]["Horas a Dever"].sum(),
                "Extras Comum": df_calculado[df_calculado["Funcionário"] == funcionario]["Horas Extras (Comum)"].sum(),
                "Extras 100%": df_calculado[df_calculado["Funcionário"] == funcionario]["Horas Extras (100%)"].sum(),
            }
            saldo_final = totals["Extras Comum"] + totals["Extras 100%"] - totals["A Dever"]

            bold_font = Font(bold=True)
            start_row = len(df_funcionario) + 3
            worksheet.cell(row=start_row, column=1, value="Resumo Mensal do Funcionário").font = bold_font

            resumo_data = [
                ("Total de Horas Normais", totals["Normais"]),
                ("Total de Horas a Dever", totals["A Dever"]),
                ("Total de Horas Extras (Comum)", totals["Extras Comum"]),
                ("Total de Horas Extras (100%)", totals["Extras 100%"]),
                ("Saldo Final de Horas", saldo_final)
            ]

            for i, (label, value) in enumerate(resumo_data, start=1):
                cell_label = worksheet.cell(row=start_row + i, column=1, value=label)
                cell_value = worksheet.cell(row=start_row + i, column=2, value=value)
                cell_value.number_format = time_format
                if "Saldo Final" in label:
                     cell_label.font = bold_font
                     cell_value.font = bold_font

            for col_letter in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
                 worksheet.column_dimensions[col_letter].width = 22

    output.seek(0)
    return output

@app.post("/converter")
async def converter_cartao_ponto(files: List[UploadFile] = File(...)):
    """ Endpoint principal que recebe um ou mais arquivos e retorna um único Excel. """
    try:
        arquivo_excel = processar_multiplos_arquivos(files)
        return StreamingResponse(
            arquivo_excel,
            media_type="application/vnd.openxmlformats-officedocument.sheet",
            headers={"Content-Disposition": f"attachment; filename=relatorio_consolidado_{datetime.now().strftime('%Y-%m-%d')}.xlsx"}
        )
    except ValueError as e:
        return {"erro": str(e)}, 400
    except Exception as e:
        return {"erro": f"Ocorreu um erro inesperado no servidor: {e}"}, 500
