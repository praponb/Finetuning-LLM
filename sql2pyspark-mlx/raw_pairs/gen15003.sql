WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
)
SELECT
    log_ts,
    latency_ms,
    status_code,
    k,
    value,
    COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h,
    MAX(latency_ms) OVER (PARTITION BY k) AS k_max_latency_ms
FROM lines;
