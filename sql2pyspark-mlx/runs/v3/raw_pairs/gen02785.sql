SELECT id, duration_sec, PERCENT_RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_pct_rank FROM web_events;
