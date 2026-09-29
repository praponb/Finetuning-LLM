SELECT id, method FROM payments WHERE HOUR(paid_at) BETWEEN 11 AND 14 EXCEPT SELECT id, method FROM payments WHERE order_id IN (21, 27, 31)
