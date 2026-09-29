WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('month', delivered_date) AS delivered_date_period,
        CAST(cost AS INT) AS cost_int
    FROM shipments
    WHERE YEAR(delivered_date) >= 2025
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END AS is_fedex,
        HOUR(delivered_date_period) AS delivered_date_period_hour
    FROM filtered
    WHERE shipped_date >= DATE '2023-08-06'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.status,
        src.cost_int,
        src.is_fedex,
        src.delivered_date_period_hour,
        p.order_date,
        p.discount,
        p.shipping_fee
    FROM cleaned src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.carrier,
        e.status,
        e.is_fedex,
        e.delivered_date_period_hour,
        e.order_date,
        e.shipping_fee,
        p.amount,
        p.discount AS order_discount
    FROM enriched e
    INNER JOIN orders p
        ON e.order_id <=> p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN delivered_date_period_hour >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS delivered_date_period_hour_tier,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT(*) FILTER (WHERE carrier = 'FedEx') AS fedex_rows,
    SIZE(COLLECT_SET(carrier)) AS num_carriers_2,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENTILE_APPROX(order_discount, ARRAY(0.25, 0.5, 0.75)) AS order_discount_quartiles
FROM with_dims
GROUP BY CASE WHEN delivered_date_period_hour >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY fedex_rows;
