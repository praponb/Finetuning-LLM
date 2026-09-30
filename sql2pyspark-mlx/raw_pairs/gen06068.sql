SELECT web_events.id, web_events.event_type, sessions.browser FROM web_events INNER JOIN sessions ON web_events.session_id = sessions.id;
