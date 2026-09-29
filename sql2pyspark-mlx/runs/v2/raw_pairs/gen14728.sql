SELECT
    id,
    method,
    paid_at,
    status,
    amount,
    NTILE(10) OVER (PARTITION BY method ORDER BY paid_at, id) AS paid_at_ntile,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS amount_pct_rank
FROM payments
