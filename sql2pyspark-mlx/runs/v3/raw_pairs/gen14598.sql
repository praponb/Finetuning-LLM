select id, event_type, date_trunc('month', event_time) event_time_period from web_events where event_time >= '2025-05-16' order by event_time_period desc, id limit 10;
