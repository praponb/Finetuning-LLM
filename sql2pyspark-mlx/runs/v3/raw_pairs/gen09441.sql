SELECT we.event_type, s.device, s.browser FROM web_events we LEFT OUTER JOIN sessions s ON s.id = we.session_id ORDER BY event_type;
