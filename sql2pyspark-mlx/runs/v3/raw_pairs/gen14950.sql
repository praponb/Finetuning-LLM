SELECT signup_date
FROM customers
EXCEPT
SELECT opened_date AS signup_date
FROM stores
WHERE region IS NOT NULL
ORDER BY signup_date
