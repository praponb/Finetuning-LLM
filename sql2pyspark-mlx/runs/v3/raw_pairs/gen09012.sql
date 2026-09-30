SELECT MIN(paid_at) AS first_paid_at, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE method IN ('cash', 'promptpay', 'paypal')
ORDER BY first_paid_at DESC
