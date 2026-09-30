WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS rn
    FROM web_events
    QUALIFY rn = 1
),
ranked AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        rn
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY duration_sec, id) = 1
),
with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessions_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        rn,
        SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
scored AS (
    SELECT
        *,
        MAX(session_no) OVER w_recent AS last7_max_session_no,
        SUM(session_no) OVER w_recent AS last7_sum_session_no,
        MIN(session_no) OVER w_recent AS last7_min_session_no
    FROM sessions_web_events
    WHERE session_no BETWEEN 2 AND 6
    WINDOW w_recent AS (PARTITION BY event_type, session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.rn,
    s.session_no,
    CASE WHEN event_type RLIKE '^(purchase|add_to_cart)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS event_type_zone,
    (SELECT AVG(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM scored s
ORDER BY rn, event_time;
