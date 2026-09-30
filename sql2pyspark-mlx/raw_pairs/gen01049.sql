select page, year(event_time) as event_year, round(avg(duration_sec), 2) as avg_duration_sec from web_events where session_id in (30, 33, 34) and page = '/cart' group by page, event_year;
