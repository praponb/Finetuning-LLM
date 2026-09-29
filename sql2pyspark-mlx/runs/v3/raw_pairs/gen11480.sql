SELECT product_id, MAX(quantity) AS max_quantity FROM inventory GROUP BY product_id ORDER BY product_id, max_quantity DESC LIMIT 1;
