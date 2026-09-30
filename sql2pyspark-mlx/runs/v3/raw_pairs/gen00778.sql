SELECT MAX(paid_at) AS latest_paid_at, SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) AS failed_amount
FROM payments
ORDER BY latest_paid_at;
