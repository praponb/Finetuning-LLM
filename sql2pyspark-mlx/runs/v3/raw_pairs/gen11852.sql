SELECT
    we.id,
    we.duration_sec,
    we.event_time,
    s.started_at,
    c.credit_limit,
    LENGTH(s.country) AS country_length
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON c.id = s.customer_id;
