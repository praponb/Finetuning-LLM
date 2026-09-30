SELECT MAX(amount) AS max_amount, MIN(amount) AS min_amount, MIN(paid_at) AS min_paid_at
FROM payments
WHERE paid_at >= '2024-11-01' AND method IN ('card', 'bank_transfer', 'cash')
ORDER BY max_amount;
