SELECT id, store_id, quantity, SUM(quantity) OVER (PARTITION BY store_id) AS store_id_sum_quantity FROM inventory;
