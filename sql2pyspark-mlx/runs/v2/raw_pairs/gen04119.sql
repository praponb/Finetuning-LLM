SELECT status FROM payments WHERE status IN ('FAILED', 'REFUNDED') EXCEPT SELECT status FROM shipments ORDER BY status
