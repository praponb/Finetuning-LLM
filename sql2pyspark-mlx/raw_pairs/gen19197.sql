SELECT
    *,
    LEAD(duration_sec) OVER w_ordered AS next_duration_sec,
    LAG(duration_sec) OVER w_ordered AS prev_duration_sec,
    ROW_NUMBER() OVER w_ordered AS event_seq
FROM web_events
WINDOW w_ordered AS (PARTITION BY event_type, session_id ORDER BY event_time, id)
