WITH session_summary AS (
    SELECT session_id, COUNT(DISTINCT duration_sec) AS unique_pages, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    WHERE page NOT IN ('/search', '/product', '/checkout')
    GROUP BY session_id
)
SELECT s.browser, ss.unique_pages, ss.max_duration_sec
FROM sessions s
LEFT OUTER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY max_duration_sec DESC
