SELECT
    id,
    event_type,
    event_time,
    PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) event_time_pct_rank
FROM web_events
WHERE session_id IN (1, 20, 25, 39);
