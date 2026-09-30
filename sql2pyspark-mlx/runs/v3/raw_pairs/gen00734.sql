SELECT id, order_id, status, amount, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY amount DESC, id) AS rn FROM payments WHERE order_id = 30
