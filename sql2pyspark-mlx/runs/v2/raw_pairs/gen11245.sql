SELECT
    s.id,
    s.started_at,
    s.customer_id,
    t.min_duration_sec
FROM sessions s
INNER JOIN (SELECT session_id, MIN(duration_sec) min_duration_sec
    FROM web_events
    WHERE event_type <> 'click'
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.min_duration_sec > 360
ORDER BY id DESC, customer_id DESC
LIMIT 1;
