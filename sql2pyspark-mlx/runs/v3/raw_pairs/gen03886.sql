SELECT *, ROUND(quantity, 2) AS quantity_rounded FROM inventory WHERE store_id IN (2, 4, 5) AND last_restock > '2025-08-14' ORDER BY id LIMIT 10;
