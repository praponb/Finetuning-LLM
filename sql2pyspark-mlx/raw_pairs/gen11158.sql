WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
)
SELECT
    id,
    host,
    status_code,
    key,
    attr_value,
    CORR(status_code, latency_ms) OVER (PARTITION BY host, service) AS corr_status_code_latency_ms,
    MAX(latency_ms) OVER w_grp AS service_max_latency_ms,
    MIN(latency_ms) OVER (PARTITION BY key, host) AS key_min_latency_ms,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
    DENSE_RANK() OVER (ORDER BY status_code DESC) AS status_code_dense_rank
FROM flattened
WINDOW w_grp AS (PARTITION BY service)
