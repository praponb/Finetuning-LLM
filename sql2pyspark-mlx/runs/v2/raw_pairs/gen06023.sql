SELECT s.device, s.browser, t.last_event_time
FROM sessions s
INNER JOIN (SELECT session_id, MAX(event_time) AS last_event_time
    FROM web_events
    WHERE TO_DATE(event_time) >= '2025-10-01'
    GROUP BY session_id) t ON s.id = t.session_id;
