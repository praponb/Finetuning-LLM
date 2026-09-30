WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY status, order_id ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS cost_avg_3_prior,
        AVG(cost) OVER (PARTITION BY carrier, status) AS carrier_avg_cost
    FROM shipments
)
SELECT
    id,
    delivered_date,
    cost,
    status,
    carrier_avg_cost
FROM metrics
ORDER BY delivered_date DESC
