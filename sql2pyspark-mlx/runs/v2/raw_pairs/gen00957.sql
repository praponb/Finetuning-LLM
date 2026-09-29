SELECT id, store_id, ROUND(quantity * 0.85, 2) quantity_adjusted FROM inventory WHERE product_id IN (4, 5, 6, 9)
