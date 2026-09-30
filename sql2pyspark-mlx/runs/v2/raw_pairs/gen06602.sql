SELECT
    we.session_id,
    s.customer_id,
    c.is_active,
    c.signup_date
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE c.age BETWEEN 43 AND 52 AND c.country != 'TH';
