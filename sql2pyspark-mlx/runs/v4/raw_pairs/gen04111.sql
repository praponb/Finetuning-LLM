SELECT MAX(quantity) - MIN(quantity) AS quantity_range, MAX(quantity) AS max_quantity, COUNT(*) AS num_inventory FROM inventory WHERE last_restock < '2023-03-21' ORDER BY quantity_range
