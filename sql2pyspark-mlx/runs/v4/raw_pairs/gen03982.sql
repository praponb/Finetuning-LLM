SELECT s.country, MAX(we.duration_sec) max_duration_sec
FROM web_events we
JOIN sessions s ON s.id = we.session_id
JOIN customers c ON s.customer_id = c.id
WHERE LENGTH(c.name) < 14
GROUP BY s.country;
