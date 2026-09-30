SELECT
    status,
    method,
    SUM(CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END) AS bank_transfer_count,
    SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) AS cash_count
FROM payments
WHERE HOUR(paid_at) BETWEEN 9 AND 15
GROUP BY status, method
ORDER BY status
