WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
)
SELECT
    id,
    service,
    host,
    key,
    v,
    MIN(status_code) OVER w_part AS running_min_status_code,
    AVG(status_code) OVER w_part AS running_avg_status_code,
    COUNT(status_code) OVER w_part AS running_count_status_code
FROM lines
WINDOW w_part AS (PARTITION BY service ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
