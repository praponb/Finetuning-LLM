SELECT
    MONTH(paid_at) AS paid_month,
    AVG(amount) AS avg_amount,
    SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM payments
GROUP BY paid_month
HAVING COUNT(*) < 1
ORDER BY avg_amount DESC, paid_month
LIMIT 20
