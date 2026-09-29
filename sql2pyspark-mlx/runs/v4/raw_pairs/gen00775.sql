select event_type, page from web_events where page in ('/home', '/cart') union all select event_type, page from web_events where to_date(event_time) < '2024-12-01' order by event_type;
