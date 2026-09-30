SELECT
    paid_at,
    order_id,
    method,
    DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label,
    ROUND(amount * 1.07, 2) AS amount_scaled
FROM payments
