SELECT id, product_id, store_id, quantity FROM inventory WHERE MONTH(last_restock) IN (3, 6, 12) AND (last_restock <= '2025-01-01' OR product_id IN (8, 9, 10));
