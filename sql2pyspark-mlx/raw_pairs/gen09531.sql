WITH session_stats AS (
    SELECT session_id, COUNT(*) AS total_count, SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count
    FROM web_events
    WHERE session_id IN (23, 31, 35)
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.country,
    ss.total_count,
    ss.search_count
FROM sessions s
LEFT JOIN session_stats ss ON s.id = ss.session_id
ORDER BY total_count DESC, country ASC;
