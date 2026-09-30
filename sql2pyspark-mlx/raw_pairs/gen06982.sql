SELECT s.customer_id, t.avg_duration_sec
FROM sessions s
LEFT OUTER JOIN (SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.avg_duration_sec > 42
ORDER BY t.avg_duration_sec DESC;
