WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
)
-- Window metrics over several different partitions
SELECT
    id,
    latency_ms,
    attr_key,
    CORR(latency_ms, status_code) OVER (PARTITION BY host) AS corr_latency_ms_status_code,
    AVG(status_code) OVER (PARTITION BY host ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_avg_2_prior,
    LAG(status_code, 1) OVER (PARTITION BY YEAR(log_ts), host ORDER BY latency_ms ASC, id ASC, attr_key ASC) AS prev_status_code
FROM lines;
