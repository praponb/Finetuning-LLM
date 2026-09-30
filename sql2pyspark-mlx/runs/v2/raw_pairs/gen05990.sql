select id, event_type, page, duration_sec, avg(duration_sec) over (partition by event_type, page) event_type_avg_duration_sec from web_events where event_type in ('purchase', 'click', 'search')
