WITH session_stats AS (
    SELECT session_id, COUNT(*) row_count
    FROM web_events
    WHERE duration_sec = 301
    GROUP BY session_id
)
SELECT s.country, s.browser, ss.row_count
FROM sessions s
LEFT JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.row_count > 6
ORDER BY row_count DESC
