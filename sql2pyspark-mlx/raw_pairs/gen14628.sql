SELECT store_name AS label, 'store' AS kind
FROM stores
UNION ALL
SELECT email AS label, 'customer' AS kind
FROM customers
WHERE is_active
ORDER BY kind;
