SELECT SUM(quantity) AS total_quantity, MAX(quantity) - MIN(quantity) AS quantity_range FROM inventory WHERE quantity <= 172 AND (store_id = 5 OR store_id = 2)
