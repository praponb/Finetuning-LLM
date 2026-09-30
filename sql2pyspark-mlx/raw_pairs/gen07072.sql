SELECT *
FROM payments
WHERE (method = 'paypal' AND paid_at < '2023-08-01') OR status IN ('FAILED', 'REFUNDED', 'SUCCESS')
ORDER BY paid_at DESC
