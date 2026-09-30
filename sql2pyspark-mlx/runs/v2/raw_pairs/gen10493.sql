SELECT store_id
FROM inventory
WHERE product_id IN (1, 2)
UNION ALL
SELECT store_id
FROM inventory
WHERE store_id IN (2, 5);
