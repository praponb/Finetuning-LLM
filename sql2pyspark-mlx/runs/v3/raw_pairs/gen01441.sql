SELECT
    web_events.id,
    web_events.event_time,
    web_events.page,
    web_events.session_id,
    sessions.device,
    sessions.country
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id;
