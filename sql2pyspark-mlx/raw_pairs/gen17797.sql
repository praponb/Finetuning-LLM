SELECT s.browser, c.email, c.city
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.age <= 50
