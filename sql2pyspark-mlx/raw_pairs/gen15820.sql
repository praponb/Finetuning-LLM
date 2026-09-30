SELECT 'customer' AS source, signup_date
FROM customers
WHERE signup_date IS NOT NULL
UNION
SELECT 'store' AS source, opened_date
FROM stores
WHERE opened_date IS NOT NULL;
