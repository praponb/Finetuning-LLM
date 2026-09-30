SELECT delivered_date
FROM shipments
EXCEPT
SELECT signup_date AS delivered_date
FROM customers
WHERE age < 31
ORDER BY delivered_date DESC
