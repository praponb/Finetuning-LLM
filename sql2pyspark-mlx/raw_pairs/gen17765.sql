select * from (select event_type, page, duration_sec from web_events where duration_sec <= 335)
pivot (sum(duration_sec) for page in ('/home', '/product', '/search', '/checkout'));
