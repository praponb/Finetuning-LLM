SELECT s.id, s.country, t.duration_sec_range
FROM sessions s
LEFT JOIN (SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.duration_sec_range > 32
