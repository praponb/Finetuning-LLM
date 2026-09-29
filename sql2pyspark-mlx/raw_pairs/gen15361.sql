SELECT id, quantity, store_id, last_restock FROM inventory WHERE store_id IN (1, 2, 5) AND last_restock <= '2023-11-01';
