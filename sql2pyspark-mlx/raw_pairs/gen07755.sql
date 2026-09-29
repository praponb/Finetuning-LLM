SELECT
    method,
    SUM(amount) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(*) AS row_count
FROM payments
WHERE status IN ('FAILED', 'REFUNDED')
GROUP BY method
HAVING SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) > 47;
