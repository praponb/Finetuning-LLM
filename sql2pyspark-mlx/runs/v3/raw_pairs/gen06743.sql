SELECT sessions.started_at, web_events.page, web_events.event_type, web_events.event_time FROM sessions INNER JOIN web_events ON web_events.session_id = sessions.id
