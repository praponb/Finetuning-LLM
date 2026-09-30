SELECT id, product_id, quantity, SUM(quantity) OVER (PARTITION BY product_id) product_id_sum_quantity FROM inventory;
