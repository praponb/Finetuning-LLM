WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.amount,
        d.channel,
        d.discount
    FROM shipments e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM with_dims
    EXCEPT ALL
    SELECT status
    FROM shipments
    WHERE order_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.status = k.status
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY discount NULLS LAST) AS discount_dense_rank,
        RANK() OVER (PARTITION BY channel ORDER BY discount NULLS FIRST) AS discount_rank,
        RANK() OVER (PARTITION BY status, channel ORDER BY cost) AS cost_rank
    FROM verified
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount >= 0.31 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    carrier,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(AVG(cost), 2) AS avg_cost_2,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS cost_per_mobile,
    SUM(CASE WHEN status = 'DELIVERED' THEN discount_rank ELSE 0 END) AS delivered_discount_rank,
    COUNT_IF(status = 'IN_TRANSIT') AS in_transit_events
FROM with_windows
GROUP BY CASE WHEN discount >= 0.31 THEN 'PREMIUM' ELSE 'STANDARD' END, carrier
ORDER BY avg_cost DESC
