SELECT method, status FROM payments WHERE status = 'FAILED' UNION SELECT method, status FROM payments WHERE method <> 'cash' ORDER BY status
