select duration_sec, event_time, page, concat(event_type, ' ', page) as event_type_page from web_events where to_date(event_time) >= '2023-07-01' and page = '/search' order by duration_sec desc;
