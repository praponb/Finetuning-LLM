SELECT
    method,
    status,
    MIN(amount) min_amount,
    ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_failed
FROM payments
WHERE order_id IS NOT NULL
GROUP BY CUBE(method, status);
