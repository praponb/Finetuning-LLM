SELECT
    method,
    MIN(paid_at) first_paid_at,
    SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) bank_transfer_amount,
    SUM(CASE WHEN method = 'cash' THEN amount ELSE 0 END) cash_amount
FROM payments
WHERE TO_DATE(paid_at) < '2025-09-20' AND amount BETWEEN 850 AND 920
GROUP BY method;
