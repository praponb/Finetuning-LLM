WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM exploded
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessions_app_logs
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST, id, key) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        new_group_flag,
        session_no,
        NTILE(5) OVER (PARTITION BY service, host ORDER BY latency_ms, id, key) latency_ms_bucket,
        NTILE(5) OVER w_part session_no_bucket
    FROM ranked
    WINDOW w_part AS (PARTITION BY key, host ORDER BY session_no, id, key)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT service
    FROM with_windows
    INTERSECT ALL
    SELECT service
    FROM app_logs
    WHERE host = 'h2'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.service = k.service
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        new_group_flag,
        session_no,
        latency_ms_bucket,
        session_no_bucket,
        SUBSTRING(level, 1, 2) level_prefix,
        LENGTH(key) key_length
    FROM reconciled
    WHERE latency_ms_bucket = 8
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        new_group_flag + key_length new_group_flag_plus_key_length,
        UPPER(key) key_upper,
        COALESCE(key, 'NONE') resolved_key
    FROM valid
    WHERE key IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    service,
    level,
    CASE WHEN new_group_flag_plus_key_length >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_plus_key_length_tier,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(level)) AS num_levels,
    MIN(key_length) AS min_key_length,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    ROUND(SUM(new_group_flag_plus_key_length) / NULLIF(COUNT(CASE WHEN host = 'h4' THEN 1 END), 0), 2) AS new_group_flag_plus_key_length_per_h4,
    DENSE_RANK() OVER (PARTITION BY service ORDER BY MIN(key_length) DESC) AS min_key_length_dense_rank
FROM base
GROUP BY service, level, CASE WHEN new_group_flag_plus_key_length >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING SIZE(COLLECT_SET(level)) > 1
ORDER BY new_group_flag_plus_key_length_tier;
