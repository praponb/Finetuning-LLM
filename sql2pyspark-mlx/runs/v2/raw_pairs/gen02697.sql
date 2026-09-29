SELECT status FROM payments WHERE TO_DATE(paid_at) >= '2025-08-01' UNION ALL SELECT status FROM shipments ORDER BY status DESC
