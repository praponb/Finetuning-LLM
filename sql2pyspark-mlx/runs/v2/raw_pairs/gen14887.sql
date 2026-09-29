SELECT id, age, country
FROM customers
WHERE is_active = TRUE
UNION
SELECT id, age, country
FROM customers
WHERE city <> 'Tokyo'
ORDER BY id DESC
