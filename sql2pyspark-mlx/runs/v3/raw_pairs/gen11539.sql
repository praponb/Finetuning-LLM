SELECT opened_date, 'store' AS source
FROM stores
UNION
SELECT signup_date, 'customer' AS source
FROM customers
