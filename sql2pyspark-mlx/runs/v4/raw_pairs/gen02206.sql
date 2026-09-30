WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-05-28')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        LEAD(units, 1, 0) OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit, id) AS next_units,
        ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS seq_no,
        LEAD(units, 1, 0) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_units_2,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        LAST_VALUE(revenue, TRUE) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue
    FROM valid
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_share NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id) AS store_id_avg_revenue,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY revenue_per_unit NULLS FIRST) AS revenue_per_unit_cume_dist
    FROM ranked
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.price,
        b.launch_date
    FROM metrics e
    LEFT JOIN products b
        ON e.product_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_units_2 >= 42 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_units_2_tier,
    CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(DISTINCT product_id) AS distinct_product_ids_2
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN next_units_2 >= 42 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN revenue >= 4200 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN next_units_2 >= 42 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY distinct_store_ids DESC NULLS LAST
