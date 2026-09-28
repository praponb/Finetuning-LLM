# Streamlit UI for the SQL -> PySpark converter.
# The model itself is served by Ollama (natively on the host by default: fast, uses the Apple GPU).
# Java + PySpark are for the optional Kimi K3 repair, which checks code on empty tables (kimi_repair.py).
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYSPARK_PYTHON=python \
    OLLAMA_URL=http://host.docker.internal:11434

WORKDIR /app
COPY docker/requirements-app.txt .
RUN pip install -r requirements-app.txt

COPY app.py kimi_repair.py ./

RUN useradd --create-home appuser
USER appuser

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=4)"

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
