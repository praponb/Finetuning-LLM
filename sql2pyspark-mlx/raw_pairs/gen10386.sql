SELECT
    method,
    MIN(paid_at) AS min_paid_at,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE order_id IS NOT NULL AND status IN ('FAILED', 'REFUNDED', 'SUCCESS')
GROUP BY method
ORDER BY avg_amount;
