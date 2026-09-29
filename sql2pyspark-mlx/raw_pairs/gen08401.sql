SELECT
    id,
    service,
    latency_ms,
    status_code,
    k,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) x AS k, v;
