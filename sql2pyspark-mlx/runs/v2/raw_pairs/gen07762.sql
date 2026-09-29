SELECT
    web_events.id,
    web_events.page,
    web_events.session_id,
    web_events.event_time
FROM web_events
WHERE web_events.duration_sec >= 452 AND web_events.session_id NOT IN (SELECT id FROM sessions WHERE device = 'desktop');
