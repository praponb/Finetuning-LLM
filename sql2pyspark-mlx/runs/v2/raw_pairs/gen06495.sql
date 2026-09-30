select * from (select event_type, page, duration_sec from web_events where page = '/home') pivot (max(duration_sec) for page in ('/cart', '/home', '/search')) order by event_type;
