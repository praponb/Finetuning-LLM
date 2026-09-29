WITH gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessionized AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.session_id_2,
    CASE WHEN event_type RLIKE '^(purchase|page_view)$' THEN 'GROUP_A' WHEN event_type RLIKE '^(click|add_to_cart)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS event_type_zone,
    (SELECT SUM(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS sum_web_event_duration_sec
FROM sessionized s
ORDER BY sum_web_event_duration_sec;
