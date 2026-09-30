WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
)
SELECT
    *,
    RANK() OVER (PARTITION BY DATE_TRUNC('week', log_ts), service ORDER BY latency_ms DESC) latency_ms_rank,
    AVG(status_code) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) status_code_avg_2h,
    CORR(latency_ms, status_code) OVER (PARTITION BY service) corr_latency_ms_status_code,
    AVG(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) latency_ms_avg_1h
FROM lines;
