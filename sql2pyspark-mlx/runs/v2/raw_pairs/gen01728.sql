SELECT
    web_events.id,
    web_events.event_time,
    web_events.session_id,
    web_events.page
FROM web_events
WHERE web_events.session_id NOT IN (SELECT id FROM sessions WHERE browser != 'Firefox')
