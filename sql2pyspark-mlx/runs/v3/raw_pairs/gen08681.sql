SELECT
    method,
    status,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
    MIN(amount) AS min_amount
FROM payments
GROUP BY ROLLUP(method, status);
