SELECT status, MAX(paid_at) AS latest_paid_at, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
GROUP BY status
HAVING avg_amount > 30
