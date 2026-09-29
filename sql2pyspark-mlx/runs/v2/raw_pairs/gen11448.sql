SELECT *
FROM inventory
WHERE store_id IN (2, 3) AND product_id IN (3, 6, 8, 10) AND product_id = 10
