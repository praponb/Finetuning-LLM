WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price / quantity, 2) line_total
    FROM order_items
    WHERE product_id IS NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.channel,
        d.status,
        d.shipping_fee
    FROM base e
    INNER JOIN orders d
        ON e.order_id = d.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY unit_price DESC NULLS FIRST, id) AS rn
    FROM joined
    QUALIFY rn = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, quantity + rn quantity_plus_rn
    FROM ranked
    WHERE unit_price IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN quantity_plus_rn >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_plus_rn_tier,
    status,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS shipping_fee_per_web,
    APPROX_PERCENTILE(line_total, 0.5) AS line_total_p50,
    SIZE(COLLECT_SET(status)) AS num_status,
    ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) AS shipping_fee_per_shipped
FROM valid
GROUP BY CASE WHEN quantity_plus_rn >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END, status;
