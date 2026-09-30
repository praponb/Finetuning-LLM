SELECT
    s.id,
    s.started_at,
    s.customer_id,
    t.stddev_duration_sec
FROM sessions s
JOIN (SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.stddev_duration_sec > 21;
