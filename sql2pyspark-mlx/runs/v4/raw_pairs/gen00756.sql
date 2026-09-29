SELECT
    method,
    ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed,
    MAX(amount) - MIN(amount) AS amount_range,
    ROUND(AVG(amount), 2) AS avg_amount
FROM payments
GROUP BY method
HAVING SUM(CASE WHEN method = 'paypal' THEN amount ELSE 0 END) < 60;
