SELECT status, MAX(paid_at) AS last_paid_at
FROM payments
WHERE method IN ('cash', 'promptpay', 'paypal')
GROUP BY status;
