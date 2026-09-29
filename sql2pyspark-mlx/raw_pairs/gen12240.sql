SELECT id, store_id, discount, MIN(discount) OVER (PARTITION BY store_id) AS store_id_min_discount FROM orders;
