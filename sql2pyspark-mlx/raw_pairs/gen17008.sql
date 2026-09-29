SELECT
    we.event_time,
    we.session_id,
    s.started_at,
    s.browser,
    c.name,
    c.country,
    TRIM(s.country) clean_country
FROM web_events we
RIGHT OUTER JOIN sessions s ON we.session_id = s.id
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
ORDER BY session_id DESC;
