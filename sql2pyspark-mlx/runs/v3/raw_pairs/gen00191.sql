SELECT id, status, method FROM payments WHERE status IN ('REFUNDED', 'SUCCESS', 'FAILED') UNION SELECT id, status, method FROM payments WHERE status = 'REFUNDED' ORDER BY id;
