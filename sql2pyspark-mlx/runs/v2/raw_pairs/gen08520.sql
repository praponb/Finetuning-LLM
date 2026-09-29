SELECT s.id, s.device, t.total_duration_sec
FROM sessions s
LEFT OUTER JOIN (SELECT session_id, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    WHERE page NOT IN ('/checkout', '/home', '/search')
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.total_duration_sec > 1000
ORDER BY s.id;
