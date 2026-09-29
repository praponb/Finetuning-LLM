select * from (select event_type, page, duration_sec from web_events where duration_sec between 142 and 319) pivot (sum(duration_sec) for page in ('/search', '/checkout', '/home', '/product'));
