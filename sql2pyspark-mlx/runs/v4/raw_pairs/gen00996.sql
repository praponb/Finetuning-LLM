SELECT store_id, product_id
FROM inventory
EXCEPT DISTINCT
SELECT store_id, product_id
FROM inventory
WHERE store_id = 5
