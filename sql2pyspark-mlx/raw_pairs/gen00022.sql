WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(brand, 1, 4) AS brand_prefix
    FROM products
    WHERE launch_date >= DATE '2023-07-09'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.unit_price > 300
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.category,
        a.price,
        a.cost,
        a.launch_date,
        a.supplier_id,
        a.brand_prefix,
        d.supplier_name,
        d.country,
        d.rating
    FROM not_flagged a
    LEFT JOIN suppliers d
        ON a.supplier_id = d.id
    WHERE d.country NOT IN ('JP', 'SG', 'TH')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock < s.launch_date
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    CASE WHEN cost >= 560 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    CORR(price, cost) AS corr_price_cost,
    SIZE(COLLECT_SET(category)) AS num_categories,
    COUNT(DISTINCT category) AS distinct_categories,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles
FROM eligible
GROUP BY DATE_TRUNC('YEAR', launch_date), CASE WHEN cost >= 560 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_categories;
