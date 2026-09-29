SELECT
    s.id,
    s.country,
    COUNT(we.id) AS web_event_count,
    COUNT(DISTINCT we.duration_sec) AS unique_pages
FROM sessions s
JOIN web_events we ON s.id = we.session_id
WHERE s.device IN ('desktop', 'mobile')
GROUP BY s.id, s.country
HAVING COUNT(DISTINCT we.duration_sec) >= 1
ORDER BY unique_pages DESC;
