WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM lines
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key, level ORDER BY log_ts DESC, id ASC, key ASC) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        SUBSTRING(level, 1, 3) AS level_prefix,
        CONCAT(key, '/', level) AS key_level
    FROM latest
    WHERE log_ts >= TIMESTAMP '2025-06-02 00:00:00'
      AND key IS NOT NULL
      AND key IN ('trace', 'region')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts ASC, id ASC, key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts ASC, id ASC, key ASC) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(is_new_session, 2) OVER (PARTITION BY level, host ORDER BY session_id ASC, id ASC, key ASC) AS next_is_new_session,
        CUME_DIST() OVER (ORDER BY is_new_session ASC NULLS FIRST) AS is_new_session_cume_dist
    FROM islands
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        session_id,
        SUM(CASE WHEN key = 'trace' THEN is_new_session ELSE 0 END) AS trace_is_new_session,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        APPROX_PERCENTILE(next_is_new_session, 0.9) AS next_is_new_session_p90,
        COUNT(*) AS cnt
    FROM with_windows
    GROUP BY session_id
)
SELECT
    session_id,
    trace_is_new_session,
    duration_min,
    key_list,
    next_is_new_session_p90,
    cnt
FROM totals
ORDER BY duration_min DESC NULLS FIRST;
