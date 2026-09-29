SELECT product_id, id
FROM store_sales
EXCEPT ALL
SELECT product_id, id
FROM store_sales
WHERE units < 33
