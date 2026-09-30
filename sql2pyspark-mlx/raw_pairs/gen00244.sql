WITH scored AS (
    SELECT
        *,
        LEAD(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec ASC, id ASC) AS next_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec ASC, id ASC) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec ASC, id ASC) AS seq_no,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM web_events
    WHERE page IN ('/checkout', '/product', '/home')
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.prev_duration_sec,
    s.seq_no,
    s.last_known_duration_sec,
    (SELECT AVG(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS avg_web_event_duration_sec
FROM scored s;
