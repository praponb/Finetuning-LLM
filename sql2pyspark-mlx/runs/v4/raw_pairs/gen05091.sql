WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('MONTH', event_time) AS event_time_period,
        COALESCE(page, 'N/A') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time_period - LAG(event_time_period) OVER (PARTITION BY page ORDER BY event_time_period, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT event_type
    FROM islands_web_events
    EXCEPT
    SELECT event_type
    FROM web_events
    WHERE page NOT IN ('/search', '/cart', '/checkout')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands_web_events s
    INNER JOIN remaining_keys k ON s.event_type = k.event_type
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY session_id, page ORDER BY new_group_flag DESC) AS new_group_flag_dense_rank,
        RANK() OVER (ORDER BY session_no) AS session_no_rank,
        MAX(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_max_90d
    FROM kept_rows
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY event_type ORDER BY new_group_flag NULLS LAST) AS new_group_flag_rank,
        RANK() OVER (ORDER BY session_no) AS session_no_rank_2,
        ROUND(new_group_flag_dense_rank / SUM(new_group_flag_dense_rank) OVER (PARTITION BY page, event_type), 4) AS new_group_flag_dense_rank_share
    FROM metrics
),
grouped AS (
    -- Grouped metrics
    SELECT
        event_type,
        CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows,
        MIN(event_time_period) AS started_at,
        SUM(duration_sec) FILTER (WHERE page = '/home') AS home_total_duration_sec
    FROM scored
    GROUP BY CUBE (event_type, CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT started_at
FROM grouped
ORDER BY started_at DESC
