SELECT method
FROM payments
EXCEPT ALL
SELECT method
FROM payments
WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS');
