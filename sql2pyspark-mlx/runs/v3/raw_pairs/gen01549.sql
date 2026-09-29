SELECT id, quantity, product_id, COALESCE(quantity, 0) AS quantity_filled FROM inventory WHERE product_id IN (1, 7) AND (last_restock > '2025-05-08' OR product_id IN (8, 9, 10))
