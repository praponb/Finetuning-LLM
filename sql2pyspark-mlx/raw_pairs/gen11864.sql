WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        COUNT(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_count_90d,
        SUM(amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM payments
    WHERE status != 'FAILED'
)
SELECT
    id,
    amount_count_90d,
    cumulative_amount
FROM enriched;
