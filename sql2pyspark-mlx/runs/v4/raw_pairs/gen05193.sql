SELECT s.customer_id, s.browser, c.country
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
