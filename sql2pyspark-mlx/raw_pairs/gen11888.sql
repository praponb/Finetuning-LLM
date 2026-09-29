SELECT store_id
FROM inventory
WHERE product_id IN (1, 2, 7)
UNION
SELECT store_id
FROM inventory
WHERE store_id IN (1, 2, 4, 6)
ORDER BY store_id DESC;
