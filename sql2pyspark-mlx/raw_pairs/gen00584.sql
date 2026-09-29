WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        SUM(is_new_session) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY session_id DESC, id ASC, key ASC) AS row_num
        FROM islands
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY log_ts DESC, id ASC, key ASC) AS rn
    FROM latest
    QUALIFY rn = 1
),
latest_app_logs AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        host,
        key,
        rn
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY row_num ASC, id ASC, key ASC) AS row_num_2
        FROM ranked
    ) t
    WHERE row_num_2 = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        host,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list,
        COUNT_IF(key = 'region') AS region_events,
        COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        GROUPING(host) AS grouping_level
    FROM latest_app_logs
    GROUP BY CUBE (host)
    HAVING version_rows < 1000
)
SELECT
    host,
    region_events,
    version_rows,
    distinct_ids,
    grouping_level
FROM daily
ORDER BY grouping_level DESC NULLS FIRST;
