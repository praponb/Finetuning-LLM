SELECT country, 'supplier' AS source
FROM suppliers
UNION
SELECT country, 'session' AS source
FROM sessions
WHERE customer_id = 16
ORDER BY country;
