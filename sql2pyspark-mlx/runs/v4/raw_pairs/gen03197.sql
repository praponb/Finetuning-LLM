SELECT
    id,
    paid_at,
    method,
    amount,
    PERCENT_RANK() OVER (ORDER BY paid_at) AS paid_at_pct_rank,
    ROUND(AVG(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_7
FROM payments
