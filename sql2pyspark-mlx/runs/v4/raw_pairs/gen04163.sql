SELECT id, quantity, ROUND(quantity * 0.85, 2) AS adjusted_quantity, ROUND(quantity * 1.1, 2) AS quantity_adjusted FROM inventory ORDER BY adjusted_quantity DESC, id LIMIT 50;
