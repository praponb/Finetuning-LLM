SELECT id, method, status, order_id, CAST(paid_at AS DATE) paid_at_day FROM payments WHERE order_id IN (5, 30, 35);
