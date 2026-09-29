SELECT revenue AS value
FROM store_sales
UNION ALL
SELECT price AS value
FROM products;
