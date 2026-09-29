select event_type, duration_sec from web_events where page is null intersect select event_type, duration_sec from web_events where duration_sec != 184 order by event_type desc;
