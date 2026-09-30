SELECT id, event_type, event_time, PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_time) event_time_pct_rank FROM web_events ORDER BY event_time;
