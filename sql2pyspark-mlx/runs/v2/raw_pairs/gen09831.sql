SELECT sessions.device, sessions.country, t.min_duration_sec
FROM sessions
LEFT OUTER JOIN (SELECT session_id, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY session_id) t ON sessions.id = t.session_id
ORDER BY device ASC
