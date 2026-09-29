WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(status_code) AS status_code_whole,
        HOUR(log_ts) AS log_ts_hour
    FROM exploded
    WHERE attr_key IS NOT NULL
      AND attr_key IN ('trace', 'version')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        attr_key,
        status_code_whole,
        log_ts_hour,
        SUM(is_new_session) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service, level ORDER BY log_ts, id, attr_key) AS latest_rank
    FROM sessions_app_logs
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    SUM(CASE WHEN host = 'h3' THEN session_id ELSE 0 END) AS h3_session_id,
    PERCENTILE_APPROX(status_code_whole, ARRAY(0.25, 0.5, 0.75)) AS status_code_whole_quartiles,
    COUNT(*) AS cnt,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN host = 'h2' THEN 1 END), 0), 2) AS session_id_per_h2
FROM latest
GROUP BY hour_window;
