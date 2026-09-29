SELECT s.started_at, t.stddev_duration_sec, t.home_count
FROM sessions s
INNER JOIN (SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count
    FROM web_events
    WHERE duration_sec IS NULL
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY started_at
