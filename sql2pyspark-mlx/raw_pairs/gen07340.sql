WITH metrics AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) AS amount_share,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY amount, id) AS amount_bucket,
        AVG(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2
    FROM payments
)
SELECT
    id,
    order_id,
    method,
    paid_at,
    amount_share,
    cumulative_amount,
    amount_bucket
FROM metrics;
