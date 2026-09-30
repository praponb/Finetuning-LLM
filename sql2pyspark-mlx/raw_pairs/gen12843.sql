SELECT
    id,
    method,
    ROUND(amount, 0) AS amount_rounded,
    UPPER(status) AS status_upper
FROM payments
WHERE order_id IS NULL OR HOUR(paid_at) BETWEEN 12 AND 18
