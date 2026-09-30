SELECT o.amount, c.country, c.city
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
ORDER BY city DESC;
