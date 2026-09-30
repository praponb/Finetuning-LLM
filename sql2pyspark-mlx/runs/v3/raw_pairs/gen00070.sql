SELECT s.country, MIN(we.duration_sec) AS min_duration_sec, COUNT(*) AS web_event_count
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE s.customer_id IN (19, 20, 22, 26)
GROUP BY s.country
ORDER BY min_duration_sec DESC, country
LIMIT 5;
