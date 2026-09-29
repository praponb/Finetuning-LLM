SELECT
    id,
    status,
    amount,
    paid_at,
    method,
    LAST_VALUE(paid_at) OVER (PARTITION BY status ORDER BY amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_paid_at,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rn
FROM payments
WHERE amount < 1600;
