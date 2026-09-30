SELECT store_id
FROM inventory
WHERE product_id IN (2, 5, 7, 9)
UNION ALL
SELECT store_id
FROM inventory
WHERE quantity <= 177;
