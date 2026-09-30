select event_type, duration_sec, hour(event_time) event_time_hour from web_events where duration_sec >= 243 and (page = '/home' or to_date(event_time) >= '2023-05-06') order by duration_sec desc
