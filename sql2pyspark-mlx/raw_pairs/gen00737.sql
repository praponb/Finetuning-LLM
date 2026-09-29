SELECT last_restock, product_id, COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE store_id IN (3, 5, 6) AND product_id IN (3, 5, 8, 9);
