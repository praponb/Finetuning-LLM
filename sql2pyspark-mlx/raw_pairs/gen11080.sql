WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUBSTRING(page, 1, 4) AS page_prefix,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-03-30 14:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
    CASE WHEN duration_sec >= 502 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    page,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    COUNT(DISTINCT page) AS distinct_pages,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 DESC) AS span_minutes_percent_rank
FROM sessionized
GROUP BY CASE WHEN session_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN duration_sec >= 502 THEN 'PREMIUM' ELSE 'STANDARD' END, page
HAVING COUNT(DISTINCT page) >= 1
ORDER BY page DESC;
