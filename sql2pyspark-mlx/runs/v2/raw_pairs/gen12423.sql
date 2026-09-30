SELECT
    web_events.id,
    web_events.session_id,
    web_events.event_time,
    sessions.country,
    sessions.started_at,
    sessions.customer_id
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id
WHERE web_events.page = '/checkout';
