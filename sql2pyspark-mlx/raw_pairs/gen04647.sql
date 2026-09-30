SELECT last_restock, quantity, product_id, CAST(quantity AS DOUBLE) AS quantity_num, ROUND(quantity * 1.15, 2) AS adjusted_quantity FROM inventory ORDER BY adjusted_quantity DESC
