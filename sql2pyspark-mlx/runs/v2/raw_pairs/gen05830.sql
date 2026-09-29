SELECT id, last_restock, product_id, ROUND(quantity * 0.85, 2) AS quantity_adjusted FROM inventory WHERE product_id IN (2, 3, 6)
