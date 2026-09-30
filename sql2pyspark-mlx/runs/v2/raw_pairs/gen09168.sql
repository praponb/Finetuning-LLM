select id, duration_sec, session_id, length(page) as page_length, hour(event_time) as event_time_hour from web_events where page not in ('/checkout', '/search') order by id desc limit 10;
