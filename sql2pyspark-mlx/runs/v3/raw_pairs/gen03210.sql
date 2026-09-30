SELECT status FROM orders UNION ALL SELECT status FROM payments WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS') ORDER BY status;
