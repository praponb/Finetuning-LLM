WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY event_time, id) AS seq_no
    FROM web_events
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.duration_sec,
    s.prev_duration_sec,
    s.seq_no,
    (SELECT MIN(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS min_web_event_duration_sec
FROM with_windows s
ORDER BY id DESC NULLS LAST, page;
