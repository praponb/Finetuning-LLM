WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM store_sales
    INTERSECT
    SELECT product_id
    FROM store_sales
    WHERE revenue IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    JOIN remaining_keys k ON s.product_id = k.product_id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.store_id,
        a.product_id,
        a.sale_date,
        a.revenue,
        s.region,
        s.store_name
    FROM kept_rows a
    JOIN stores s
        ON a.store_id = s.id
),
joined AS (
    -- Attach products attributes
    SELECT e.*, p.cost
    FROM enriched e
    LEFT JOIN products p
        ON e.product_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY region), 4) AS cost_share,
        CUME_DIST() OVER (ORDER BY cost DESC NULLS LAST) AS cost_cume_dist,
        ROUND(cost / SUM(cost) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), store_id), 4) AS cost_share_2
    FROM joined
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(cost_cume_dist, cost_share_2) AS greatest_cost_cume_dist_cost_share_2,
        cost_share * cost AS cost_share_times_cost
    FROM scored
    WHERE sale_date > DATE '2023-03-28'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', sale_date), product_id ORDER BY cost_share_2 DESC) AS cost_share_2_percent_rank,
        CUME_DIST() OVER (ORDER BY cost_share_times_cost) AS cost_share_times_cost_cume_dist
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    CASE WHEN revenue >= 3700 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    PERCENTILE_APPROX(cost_cume_dist, 0.5) AS cost_cume_dist_p50,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    MAX(cost_share_times_cost_cume_dist) FILTER (WHERE region = 'South') AS south_max_cost_share_times_cost_cume_dist,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list_2,
    ROUND(AVG(cost_share_2), 2) AS avg_cost_share_2,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list_3
FROM with_windows
GROUP BY DATE_TRUNC('MONTH', sale_date), DATE_TRUNC('QUARTER', sale_date), CASE WHEN revenue >= 3700 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(AVG(cost_share_2), 2) >= 0
ORDER BY revenue_tier DESC NULLS LAST
