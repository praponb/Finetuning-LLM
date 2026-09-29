SELECT id, product_id, quantity, AVG(quantity) OVER (PARTITION BY product_id) product_id_avg_quantity FROM inventory WHERE quantity <= 217 ORDER BY quantity DESC, product_id;
