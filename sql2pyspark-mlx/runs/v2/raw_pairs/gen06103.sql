SELECT
    method,
    DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month,
    ROUND((100 * SUM(CASE WHEN method = 'paypal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_paypal,
    AVG(amount) AS avg_amount,
    COUNT(DISTINCT amount) AS unique_orders
FROM payments
WHERE method = 'promptpay' AND (method IN ('bank_transfer', 'paypal') OR amount <= 570)
GROUP BY method, DATE_FORMAT(paid_at, 'yyyy-MM')
