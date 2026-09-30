SELECT id, method, CAST(amount AS INT) AS amount_int
FROM payments
WHERE paid_at < '2025-09-01' AND method IN ('promptpay', 'paypal')
