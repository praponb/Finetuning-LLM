SELECT quantity, store_id
FROM inventory
WHERE quantity IS NOT NULL AND (store_id IN (3, 6) OR product_id IN (2, 5, 7))
ORDER BY store_id;
