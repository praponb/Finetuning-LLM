SELECT id, status FROM payments WHERE status = 'FAILED' UNION ALL SELECT id, status FROM payments WHERE status != 'SUCCESS'
