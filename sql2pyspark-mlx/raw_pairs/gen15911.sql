SELECT
    id,
    event_type,
    duration_sec,
    PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS duration_sec_pct_rank
FROM web_events
