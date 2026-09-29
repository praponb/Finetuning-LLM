SELECT
    id,
    session_id,
    event_type,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY event_time, id) AS row_num
FROM web_events
ORDER BY id;
