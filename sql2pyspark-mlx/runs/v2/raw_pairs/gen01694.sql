SELECT
    method,
    status,
    COUNT(DISTINCT amount) AS unique_orders,
    SUM(amount) AS total_amount
FROM payments
WHERE status IN ('REFUNDED', 'SUCCESS') AND method = 'paypal' AND TO_DATE(paid_at) < '2024-12-24'
GROUP BY method, status
