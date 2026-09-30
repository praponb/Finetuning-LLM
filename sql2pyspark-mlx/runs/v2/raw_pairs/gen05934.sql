SELECT
    web_events.id,
    web_events.duration_sec,
    web_events.event_type,
    web_events.session_id,
    sessions.device,
    sessions.started_at,
    sessions.customer_id
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.country NOT IN ('DE', 'UK') OR web_events.event_type = 'purchase'
ORDER BY session_id, web_events.id;
