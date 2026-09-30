SELECT product_id, id
FROM store_sales
EXCEPT
SELECT product_id, id
FROM store_sales
WHERE units <= 16;
