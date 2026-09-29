SELECT id, status, TRIM(status) clean_status
FROM payments
ORDER BY status, id
LIMIT 10
