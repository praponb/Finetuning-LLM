SELECT c.is_active, MIN(we.event_time) AS first_event_time, MAX(we.event_time) AS latest_event_time
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active
