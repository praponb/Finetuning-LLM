SELECT ROUND(STDDEV(amount), 2) stddev_amount, SUM(amount) total_amount
FROM payments
WHERE method IS NOT NULL AND paid_at < '2025-05-01'
ORDER BY stddev_amount DESC, total_amount DESC
