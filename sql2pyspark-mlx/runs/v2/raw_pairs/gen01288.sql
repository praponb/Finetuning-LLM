SELECT
    DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month,
    method,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash,
    MIN(amount) AS min_amount
FROM payments
WHERE order_id IN (12, 18, 21, 40)
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM'), method
HAVING SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) >= 78
ORDER BY paid_month DESC
