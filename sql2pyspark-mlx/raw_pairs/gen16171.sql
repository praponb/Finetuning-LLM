SELECT
    id,
    session_id,
    duration_sec,
    ROW_NUMBER() OVER (ORDER BY event_time, id) AS event_seq,
    COUNT(duration_sec) OVER (PARTITION BY event_type) AS event_type_count_duration_sec,
    NTILE(4) OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS duration_sec_bucket
FROM web_events;
