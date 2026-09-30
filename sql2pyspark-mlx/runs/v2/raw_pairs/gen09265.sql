SELECT
    id,
    status,
    paid_at,
    RANK() OVER (PARTITION BY status ORDER BY paid_at) rnk
FROM payments
ORDER BY paid_at;
