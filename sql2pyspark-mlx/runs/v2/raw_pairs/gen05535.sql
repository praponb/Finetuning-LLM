SELECT id, status
FROM payments
WHERE status IN ('FAILED', 'SUCCESS')
EXCEPT
SELECT id, status
FROM payments
WHERE method = 'bank_transfer'
ORDER BY status DESC;
