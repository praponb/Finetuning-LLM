SELECT id, event_time, PERCENT_RANK() OVER (ORDER BY event_time) AS event_time_pct_rank FROM web_events ORDER BY event_time_pct_rank DESC
