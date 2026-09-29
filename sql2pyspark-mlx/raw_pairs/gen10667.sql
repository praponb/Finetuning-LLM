SELECT store_id
FROM store_sales
WHERE product_id = 5
UNION ALL
SELECT store_id
FROM store_sales
WHERE units = 25
