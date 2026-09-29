SELECT
    id,
    order_id,
    method,
    paid_at,
    status,
    LEAD(amount, 2) OVER (PARTITION BY status, method ORDER BY amount, id) AS next_amount,
    CUME_DIST() OVER (PARTITION BY TO_DATE(paid_at) ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist
FROM payments
WHERE amount > 840
