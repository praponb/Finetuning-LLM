SELECT
    web_events.id,
    web_events.event_time,
    web_events.duration_sec,
    sessions.country,
    sessions.device,
    customers.country AS customer_country
FROM web_events
FULL OUTER JOIN sessions ON web_events.session_id = sessions.id
LEFT OUTER JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.device = 'tablet'
ORDER BY web_events.event_time;
