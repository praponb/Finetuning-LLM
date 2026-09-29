WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_id_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_2_tier,
    event_type,
    SIZE(COLLECT_SET(page)) AS num_pages,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows
FROM sessions_web_events
GROUP BY 1, 2
HAVING search_rows >= 1;
