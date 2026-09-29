select id, event_type, duration_sec from web_events where event_time < '2024-12-01' except select id, event_type, duration_sec from web_events where event_type = 'search'
