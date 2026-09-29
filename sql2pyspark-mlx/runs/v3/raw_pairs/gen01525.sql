WITH session_stats AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.browser,
    s.country,
    ss.page_values,
    ss.stddev_duration_sec
FROM sessions s
LEFT OUTER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.stddev_duration_sec >= 42
