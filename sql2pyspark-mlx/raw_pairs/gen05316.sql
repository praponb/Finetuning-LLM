SELECT amount AS value
FROM orders
WHERE store_id IS NOT NULL
UNION
SELECT revenue AS value
FROM store_sales
WHERE units = 16
ORDER BY value;
