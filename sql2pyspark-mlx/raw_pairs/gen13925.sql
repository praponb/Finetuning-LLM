WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessionized AS (
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
SELECT
    id,
    event_time,
    duration_sec,
    session_id_2
FROM sessionized;
