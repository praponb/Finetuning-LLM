SELECT status, 'order' source FROM orders UNION ALL SELECT status, 'payment' source FROM payments WHERE HOUR(paid_at) BETWEEN 8 AND 16 ORDER BY status DESC
