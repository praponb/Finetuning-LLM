SELECT id, order_id, COUNT(*) OVER (PARTITION BY order_id) order_id_count FROM payments WHERE amount <= 1410
