WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM flattened
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN host = 'h4' THEN 1 ELSE 0 END AS is_h4
    FROM islands
    WHERE log_ts > DATE '2025-06-01'
      AND attr_key IS NOT NULL
      AND HOUR(log_ts) BETWEEN 8 AND 16
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host, attr_key ORDER BY status_code DESC, id, attr_key) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        is_new_burst,
        island_id,
        is_h4,
        row_num,
        LAG(is_new_burst, 2) OVER (PARTITION BY host ORDER BY latency_ms, id, attr_key) AS prev_is_new_burst,
        MAX(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_max_30m
    FROM ranked
)
SELECT
    id,
    log_ts,
    latency_ms,
    status_code,
    attr_key,
    attr_value,
    row_num,
    prev_is_new_burst,
    CASE WHEN attr_key RLIKE '^(region|version)$' THEN 'OTHER_ZONE' WHEN attr_key RLIKE '^(trace)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS attr_key_zone
FROM enriched
ORDER BY attr_key DESC;
