SELECT MAX(quantity) max_quantity
FROM inventory
WHERE (product_id IN (1, 5, 9, 10) AND product_id IN (6, 7)) OR product_id IN (2, 5, 7, 9)
ORDER BY max_quantity DESC;
