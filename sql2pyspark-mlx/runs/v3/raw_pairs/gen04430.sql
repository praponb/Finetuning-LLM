SELECT
    we.id,
    we.session_id,
    s.customer_id,
    s.device,
    c.credit_limit
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
RIGHT JOIN customers c ON s.customer_id = c.id
ORDER BY we.id DESC;
