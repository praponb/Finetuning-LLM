select session_id, page from web_events where hour(event_time) between 7 and 11 union all select session_id, page from web_events where hour(event_time) between 8 and 15;
