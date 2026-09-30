SELECT id, store_id FROM inventory WHERE last_restock > '2023-06-01' UNION ALL SELECT id, store_id FROM inventory WHERE last_restock <= '2023-07-01' ORDER BY id
