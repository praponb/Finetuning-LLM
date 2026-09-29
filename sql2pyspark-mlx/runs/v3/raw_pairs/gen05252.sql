SELECT
    id,
    session_id,
    duration_sec,
    event_type,
    event_time,
    PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS duration_sec_pct_rank,
    RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) AS event_time_rank
FROM web_events
ORDER BY event_time_rank DESC;
