select id, page, event_type, event_time from web_events where hour(event_time) between 8 and 15 and session_id in (1, 8, 20, 36) order by event_time, id limit 5
