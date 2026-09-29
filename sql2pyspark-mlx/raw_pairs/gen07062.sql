WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM exploded
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('week', log_ts) AS log_ts_period,
        CAST(session_start_flag AS STRING) AS session_start_flag_num
    FROM sessions_app_logs
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 14:00:00')
      AND attr_key IS DISTINCT FROM 'region'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(session_id, status_code) AS least_session_id_status_code
    FROM valid
    WHERE log_ts >= TIMESTAMP '2025-06-01 11:00:00'
      AND attr_key IS NOT DISTINCT FROM 'version'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        session_id,
        log_ts_period,
        least_session_id_status_code
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY session_id, id, attr_key) AS dedup_rank
        FROM base
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    log_ts,
    level,
    latency_ms,
    status_code,
    attr_key,
    session_id,
    log_ts_period,
    least_session_id_status_code
FROM deduped;
