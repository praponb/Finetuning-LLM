SELECT country, segment, is_active
FROM customers
WHERE age >= 62 AND credit_limit <= 8200
ORDER BY segment;
