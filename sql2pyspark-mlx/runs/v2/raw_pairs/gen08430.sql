SELECT
    method,
    status,
    MAX(amount) AS max_amount,
    SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount
FROM payments
WHERE status = 'FAILED' OR status = 'REFUNDED'
GROUP BY CUBE(method, status);
