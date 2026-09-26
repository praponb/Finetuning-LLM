# Data pipeline + evaluation tooling: generate pairs, execute them in local Spark, split, evaluate.
# The project folder is mounted at /workspace, so outputs land in the normal place on the host.
# (Training/fusing need Apple MLX + Metal and therefore run natively on the Mac, not here.)
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless make procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYSPARK_PYTHON=python \
    OLLAMA_URL=http://host.docker.internal:11434

COPY docker/requirements-tools.txt /tmp/requirements-tools.txt
RUN pip install -r /tmp/requirements-tools.txt

WORKDIR /workspace
CMD ["bash"]
