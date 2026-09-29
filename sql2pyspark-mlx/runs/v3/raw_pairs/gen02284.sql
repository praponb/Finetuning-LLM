SELECT id, country, age
FROM customers
WHERE is_active = TRUE
ORDER BY country DESC, 3 DESC, id
LIMIT 10;
