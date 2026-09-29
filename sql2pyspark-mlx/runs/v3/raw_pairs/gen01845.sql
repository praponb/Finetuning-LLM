SELECT
    web_events.id,
    web_events.page,
    web_events.session_id,
    sessions.started_at,
    sessions.device,
    customers.city,
    customers.name,
    UPPER(web_events.event_type) AS event_type_upper
FROM web_events
LEFT OUTER JOIN sessions ON web_events.session_id = sessions.id
LEFT OUTER JOIN customers ON sessions.customer_id = customers.id;
