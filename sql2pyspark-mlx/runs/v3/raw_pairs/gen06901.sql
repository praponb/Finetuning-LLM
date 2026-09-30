select id, event_time, duration_sec, cast(event_time as date) as event_time_day from web_events where event_time < '2024-05-01' and page not in ('/home', '/product');
