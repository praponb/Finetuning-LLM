SELECT s.country, COUNT(we.id) AS web_event_count
FROM sessions s
JOIN web_events we ON we.session_id = s.id
WHERE s.device IN ('mobile', 'desktop')
GROUP BY s.country
HAVING COUNT(we.id) >= 4;
