SELECT id, product_id, last_restock FROM inventory WHERE last_restock <= '2025-10-01' OR product_id IN (1, 3, 5, 8)
