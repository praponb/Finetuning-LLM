WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
flattened_attrs AS (
    SELECT
        id,
        host,
        latency_ms,
        key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM lines
    WHERE latency_ms BETWEEN 1177 AND 1636
),
metrics AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_percent_rank,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank
    FROM flattened_attrs
    WHERE has_fault <= 0
),
with_windows AS (
    SELECT
        host,
        key,
        value,
        has_fault,
        CUME_DIST() OVER w_running AS status_code_high_cume_dist
    FROM metrics
    WINDOW w_running AS (PARTITION BY host, key ORDER BY status_code_high DESC)
)
SELECT
    host,
    key,
    has_fault,
    status_code_high_cume_dist
FROM with_windows
ORDER BY key DESC;
