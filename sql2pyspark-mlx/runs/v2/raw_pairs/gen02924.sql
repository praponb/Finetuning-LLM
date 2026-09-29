select id, duration_sec, event_type, page from web_events where duration_sec <= (select min(duration_sec) from web_events)
