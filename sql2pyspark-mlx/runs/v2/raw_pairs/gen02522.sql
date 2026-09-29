SELECT c.credit_limit, o.channel
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE o.channel IS NULL
ORDER BY credit_limit DESC;
