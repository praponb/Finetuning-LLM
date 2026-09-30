SELECT web_events.id, web_events.event_time FROM web_events WHERE web_events.session_id IN (SELECT id FROM sessions WHERE browser IN ('Edge', 'Chrome'));
