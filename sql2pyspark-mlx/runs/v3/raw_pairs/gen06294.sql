SELECT method, status FROM payments WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED') UNION ALL SELECT method, status FROM payments WHERE TO_DATE(paid_at) >= '2025-06-01';
