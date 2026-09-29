SELECT s.device, s.country, COUNT(CASE WHEN we.event_type = 'page_view' THEN 1 END) AS page_view_count
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
GROUP BY s.device, s.country
ORDER BY s.country, page_view_count DESC
