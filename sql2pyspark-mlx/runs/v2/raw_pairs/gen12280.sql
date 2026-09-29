SELECT
    id,
    method,
    status,
    paid_at,
    amount,
    SUM(amount) OVER (PARTITION BY method, status ORDER BY paid_at, id) AS running_amount,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY paid_at) AS paid_at_pct_rank
FROM payments
