SELECT signup_date
FROM customers
WHERE LOWER(TRIM(name)) = 'frank miller'
UNION
SELECT shipped_date
FROM shipments
ORDER BY signup_date DESC
