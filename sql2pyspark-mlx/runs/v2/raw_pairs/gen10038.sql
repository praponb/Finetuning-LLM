SELECT id, email
FROM customers
WHERE is_active = TRUE
UNION
SELECT id, email
FROM customers
WHERE NOT is_active
ORDER BY id;
