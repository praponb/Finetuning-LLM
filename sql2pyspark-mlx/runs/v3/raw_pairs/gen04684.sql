SELECT
    we.id,
    we.event_time,
    s.browser,
    s.country,
    c.email,
    UPPER(s.country) AS country_upper
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id;
