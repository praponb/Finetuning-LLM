SELECT
    we.event_time,
    s.browser,
    c.segment,
    c.is_active,
    c.age + c.credit_limit AS age_plus_credit_limit
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON s.customer_id = c.id;
