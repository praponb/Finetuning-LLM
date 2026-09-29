SELECT product_id, store_id
FROM store_sales
WHERE revenue >= 2000
UNION ALL
SELECT product_id, store_id
FROM store_sales
WHERE product_id IN (3, 4);
