SELECT sessions.customer_id, web_events.page, web_events.event_type
FROM sessions
INNER JOIN web_events ON web_events.session_id = sessions.id
