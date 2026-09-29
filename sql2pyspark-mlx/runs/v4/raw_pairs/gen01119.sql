SELECT MONTH(paid_at) paid_month, status, SUM(amount) total_amount
FROM payments
GROUP BY MONTH(paid_at), status
ORDER BY status
