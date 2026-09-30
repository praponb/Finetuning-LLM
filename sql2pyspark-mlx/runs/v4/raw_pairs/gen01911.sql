SELECT status
FROM payments
WHERE status IN ('SUCCESS', 'FAILED')
EXCEPT
SELECT status
FROM payments
WHERE method IN ('card', 'cash', 'bank_transfer')
ORDER BY status;
