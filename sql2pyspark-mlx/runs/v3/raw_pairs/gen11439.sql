SELECT
    we.id,
    we.session_id,
    s.browser,
    c.segment,
    c.signup_date
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
FULL OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.device = 'desktop'
