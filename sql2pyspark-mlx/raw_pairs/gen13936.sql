SELECT
    s.customer_id,
    s.started_at,
    s.browser,
    we.page,
    DATE_FORMAT(we.event_time, 'MMM yyyy') AS event_time_label
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE we.duration_sec > 272 AND s.device = 'mobile';
