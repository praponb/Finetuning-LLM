SELECT id, method FROM payments WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED') ORDER BY method, id
