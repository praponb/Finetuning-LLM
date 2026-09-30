SELECT
    id,
    session_id,
    duration_sec,
    PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec) AS duration_sec_pct_rank
FROM web_events
