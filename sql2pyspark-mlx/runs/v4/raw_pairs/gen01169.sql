WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC) AS event_seq,
        AVG(cost) OVER (PARTITION BY category) AS category_avg_cost,
        SUM(stock_qty) OVER (ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        SUM(cost) OVER (PARTITION BY supplier_id, category ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        MAX(price) OVER (PARTITION BY brand) AS brand_max_price
    FROM products
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY brand_max_price DESC NULLS LAST, id ASC) = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.unit_price <= 230
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        b.country,
        b.rating,
        b.supplier_name
    FROM not_flagged s
    LEFT JOIN suppliers b
        ON s.supplier_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN brand_max_price >= 1069 THEN 'HIGH' ELSE 'LOW' END AS brand_max_price_tier,
    CASE WHEN cost >= 680 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    SIZE(COLLECT_SET(category)) AS num_categories,
    APPROX_PERCENTILE(cumulative_stock_qty, ARRAY(0.25, 0.5, 0.75)) AS cumulative_stock_qty_quartiles,
    GROUPING(CASE WHEN brand_max_price >= 1069 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM enriched
GROUP BY CUBE (CASE WHEN brand_max_price >= 1069 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cost >= 680 THEN 'PREMIUM' ELSE 'STANDARD' END)
