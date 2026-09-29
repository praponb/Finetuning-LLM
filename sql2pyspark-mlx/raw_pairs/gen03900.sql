WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label,
        COALESCE(page, 'NONE') AS page_filled
    FROM web_events
    WHERE event_type <> 'add_to_cart'
      AND page IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    island_id,
    event_type,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    COUNT_IF(page = '/home') AS home_events,
    SUM(CASE WHEN event_type = 'click' THEN session_start_flag ELSE 0 END) AS click_session_start_flag,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS duration_sec_per_search,
    AVG(session_start_flag) FILTER (WHERE event_type = 'page_view') AS page_view_avg_session_start_flag,
    GROUPING(island_id) AS gid
FROM sessions_web_events
GROUP BY CUBE (island_id, event_type, DATE_TRUNC('WEEK', event_time))
