SELECT city, 'customer' AS source
FROM customers
WHERE is_active
UNION ALL
SELECT city, 'store' AS source
FROM stores
ORDER BY city;
