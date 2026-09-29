WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
deduped AS (
    SELECT
        id,
        log_ts,
        key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key, host ORDER BY latency_ms DESC NULLS FIRST, id, key) AS rn
        FROM lines
    ) t
    WHERE rn = 1
),
filtered AS (
    SELECT *, MONTH(log_ts) AS log_ts_month
    FROM deduped
    WHERE key IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    SELECT
        id,
        log_ts,
        key,
        SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT id, key
FROM islands
ORDER BY id NULLS LAST
