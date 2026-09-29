SELECT
    s.id,
    s.device,
    s.browser,
    t.min_duration_sec,
    t.pct_add_to_cart
FROM sessions s
LEFT JOIN (SELECT session_id, MIN(duration_sec) AS min_duration_sec, ROUND((100 * SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart
    FROM web_events
    WHERE session_id IN (3, 22, 38)
    GROUP BY session_id) t ON s.id = t.session_id;
