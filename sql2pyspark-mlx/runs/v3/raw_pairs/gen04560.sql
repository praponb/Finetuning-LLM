SELECT id, product_id, ROUND(quantity, 1) AS quantity_rounded
FROM inventory
WHERE quantity <= 88 AND store_id IN (1, 6)
