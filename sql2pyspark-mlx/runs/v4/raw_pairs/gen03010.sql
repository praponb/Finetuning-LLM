SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    LEAD(amount, 1, 0) OVER (PARTITION BY status, method ORDER BY paid_at, id) next_amount,
    CUME_DIST() OVER (PARTITION BY status, order_id ORDER BY amount DESC NULLS LAST) amount_cume_dist,
    MAX(amount) OVER (PARTITION BY method) method_max_amount
FROM payments
