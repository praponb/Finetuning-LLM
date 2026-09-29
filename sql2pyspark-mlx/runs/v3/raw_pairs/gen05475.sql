SELECT id, status, method
FROM payments
WHERE HOUR(paid_at) BETWEEN 6 AND 14
UNION ALL
SELECT id, status, method
FROM payments
WHERE method IN ('bank_transfer', 'paypal')
ORDER BY id DESC;
