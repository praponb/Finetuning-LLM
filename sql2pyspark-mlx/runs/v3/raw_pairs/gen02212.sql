SELECT id, status, COUNT(*) OVER (PARTITION BY status) status_count
FROM payments
ORDER BY status DESC
