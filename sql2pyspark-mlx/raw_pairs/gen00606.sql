SELECT
    id,
    method,
    amount,
    PERCENT_RANK() OVER (PARTITION BY method ORDER BY amount) AS amount_pct_rank
FROM payments;
