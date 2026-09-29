SELECT web_events.event_time, sessions.device, customers.country
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
LEFT JOIN customers ON sessions.customer_id = customers.id
ORDER BY device;
