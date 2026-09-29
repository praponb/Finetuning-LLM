SELECT product_id, store_id
FROM inventory
WHERE quantity >= 104
UNION
SELECT product_id, store_id
FROM inventory
WHERE quantity < 122;
