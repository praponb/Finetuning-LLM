SELECT 'store_sale' AS origin, revenue AS value
FROM store_sales
UNION ALL
SELECT 'product' AS origin, price AS value
FROM products
ORDER BY value;
