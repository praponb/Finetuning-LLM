SELECT
    status,
    order_id,
    DATE_TRUNC('MONTH', paid_at) AS paid_month,
    MAX(paid_at) AS latest_paid_at,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE status = 'REFUNDED' AND status IN ('FAILED', 'SUCCESS', 'REFUNDED')
GROUP BY status, order_id, DATE_TRUNC('MONTH', paid_at)
HAVING stddev_amount > 61
ORDER BY order_id DESC;
