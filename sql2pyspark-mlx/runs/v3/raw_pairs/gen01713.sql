SELECT status FROM payments UNION ALL SELECT status FROM shipments WHERE status IN ('LOST', 'RETURNED', 'DELIVERED') ORDER BY status
