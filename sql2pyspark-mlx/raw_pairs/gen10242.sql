SELECT
    we.id,
    we.page,
    we.event_type,
    s.country,
    s.device,
    c.credit_limit,
    c.signup_date,
    LENGTH(s.browser) AS browser_length
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE we.session_id IN (23, 28, 33) AND c.email IS NOT NULL
