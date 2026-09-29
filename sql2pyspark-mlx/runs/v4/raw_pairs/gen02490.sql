SELECT s.country, COUNT(we.id) AS web_event_count
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.country NOT IN ('JP', 'US')
GROUP BY s.country
HAVING COUNT(we.id) >= 3
ORDER BY country
LIMIT 5;
