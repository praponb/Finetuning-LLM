SELECT o.amount, s.region, c.credit_limit
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE MONTH(c.signup_date) IN (2, 7, 10)
ORDER BY s.region, credit_limit DESC;
