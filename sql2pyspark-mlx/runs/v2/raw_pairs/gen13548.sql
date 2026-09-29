SELECT
    web_events.page,
    web_events.session_id,
    web_events.event_time,
    sessions.device,
    sessions.started_at,
    sessions.browser
FROM web_events
RIGHT OUTER JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.customer_id = 27 OR sessions.browser = 'Safari'
ORDER BY web_events.event_time;
