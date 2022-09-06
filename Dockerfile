FROM python:3.9-slim-buster AS builder

ARG PIPSERVER_URL 
ARG PIPSERVER_USERNAME 
ARG PIPSERVER_PASSWORD

WORKDIR /app

RUN python -m pip install --upgrade pip && \
    python -m pip install poetry && \
    poetry config repositories.radar $PIPSERVER_URL && \
    poetry config http-basic.radar $PIPSERVER_USERNAME $PIPSERVER_PASSWORD && \
    python -m venv /venv

COPY poetry.lock pyproject.toml ./

RUN . /venv/bin/activate && poetry install --no-dev --no-root

COPY . .

FROM python:3.9-slim-buster

WORKDIR /app

COPY --from=builder /venv /venv
COPY --from=builder /app /app

EXPOSE 5000

CMD . /venv/bin/activate && python3 -u app/main.py