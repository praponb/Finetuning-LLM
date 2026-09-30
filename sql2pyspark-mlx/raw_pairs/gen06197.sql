SELECT brand
FROM products
UNION ALL
SELECT txn_type AS brand
FROM transactions
ORDER BY brand;
