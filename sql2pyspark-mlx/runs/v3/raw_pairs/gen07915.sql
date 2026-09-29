SELECT id, product_id, last_restock, quantity, LAG(quantity, 2) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS prev_quantity FROM inventory WHERE YEAR(last_restock) = 2025;
