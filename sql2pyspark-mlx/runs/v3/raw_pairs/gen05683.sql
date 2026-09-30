SELECT s.browser, s.started_at, t.checkout_duration_sec
FROM sessions s
INNER JOIN (SELECT session_id, SUM(CASE WHEN page = '/checkout' THEN duration_sec ELSE 0 END) AS checkout_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
