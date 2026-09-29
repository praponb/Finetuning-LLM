SELECT
    id,
    paid_at,
    method,
    amount,
    order_id,
    ROUND(amount / SUM(amount) OVER (PARTITION BY TO_DATE(paid_at), method), 3) AS amount_share,
    COUNT(*) OVER (PARTITION BY DATE_TRUNC('month', paid_at), order_id) AS paid_at_period_count
FROM payments;
