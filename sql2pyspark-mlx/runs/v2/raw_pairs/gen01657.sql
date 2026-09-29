SELECT id, status, order_id FROM payments WHERE order_id IS NOT NULL EXCEPT SELECT id, status, order_id FROM payments WHERE status NOT IN ('REFUNDED', 'SUCCESS', 'FAILED')
