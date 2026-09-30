SELECT
    web_events.id,
    web_events.session_id,
    web_events.event_time,
    sessions.device,
    sessions.customer_id,
    sessions.country
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id
