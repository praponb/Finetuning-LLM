select id, duration_sec, session_id, event_time from web_events where duration_sec between 357 and 375 and to_date(event_time) >= '2024-05-01' and page = '/checkout' order by id;
