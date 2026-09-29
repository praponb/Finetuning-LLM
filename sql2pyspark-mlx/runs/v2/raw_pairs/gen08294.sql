SELECT
    web_events.event_time,
    web_events.session_id,
    web_events.duration_sec,
    sessions.started_at,
    sessions.browser
FROM web_events
FULL OUTER JOIN sessions ON web_events.session_id = sessions.id;
