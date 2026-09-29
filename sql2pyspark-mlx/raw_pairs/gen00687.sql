SELECT
    web_events.id,
    web_events.page,
    web_events.event_time,
    sessions.device,
    TRIM(web_events.event_type) clean_event_type
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id;
