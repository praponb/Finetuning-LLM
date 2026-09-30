SELECT s.id, s.started_at, c.signup_date
FROM sessions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE LENGTH(c.name) > 8
