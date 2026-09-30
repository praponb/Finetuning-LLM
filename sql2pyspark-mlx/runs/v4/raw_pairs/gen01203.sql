SELECT
    we.session_id,
    we.page,
    s.customer_id,
    s.browser,
    c.signup_date
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id;
