SELECT city, 'customer' AS source
FROM customers
UNION ALL
SELECT city, 'store' AS source
FROM stores
WHERE region IN ('Central', 'West', 'East')
ORDER BY city DESC;
