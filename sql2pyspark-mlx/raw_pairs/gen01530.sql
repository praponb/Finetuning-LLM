select * from (select page, event_type, duration_sec from web_events where event_time < '2025-09-01') pivot (sum(duration_sec) for event_type in ('purchase', 'search', 'click')) order by page
