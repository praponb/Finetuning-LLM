SELECT country
FROM customers
WHERE is_active = TRUE
UNION
SELECT country
FROM customers
WHERE is_active = TRUE;
