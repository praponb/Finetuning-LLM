WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(TRIM(store_name)) AS store_name_normalized,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
      AND city IN ('Singapore', 'Berlin')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        opened_date,
        sqft,
        region_filled
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) = 1
),
base_stores AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN sqft >= 13064 THEN 'High' WHEN sqft >= 8039 THEN 'Medium' ELSE 'Low' END AS sqft_tier,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM ranked
    WHERE opened_date >= DATE '2023-06-02'
      AND sqft < 15638
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region_filled,
        x.revenue
    FROM base_stores s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(revenue, 2) OVER (ORDER BY revenue, id) AS prev_revenue,
        SUM(revenue) OVER w_grp AS cumulative_revenue,
        NTILE(5) OVER w_grp_2 AS revenue_bucket,
        LAST_VALUE(revenue, TRUE) OVER w_grp AS last_known_revenue
    FROM with_last
    WHERE revenue IS NOT NULL
    WINDOW w_grp AS (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (ORDER BY revenue DESC, id)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date BETWEEN '2023-04-01' AND '2024-06-01'
        )
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_revenue >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_revenue_tier,
        CASE WHEN cumulative_revenue >= 22140 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_revenue_tier,
        COUNT(*) AS event_count,
        SUM(prev_revenue) AS total_prev_revenue,
        PERCENTILE_APPROX(cumulative_revenue, 0.95) AS cumulative_revenue_p95,
        ROUND(AVG(revenue), 2) AS avg_revenue,
        APPROX_PERCENTILE(revenue, 0.9) AS revenue_p90,
        ROUND(AVG(cumulative_revenue), 2) AS avg_cumulative_revenue,
        GROUPING(CASE WHEN prev_revenue >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
    FROM kept
    GROUP BY CUBE (CASE WHEN prev_revenue >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN cumulative_revenue >= 22140 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    event_count,
    total_prev_revenue,
    revenue_p90,
    avg_cumulative_revenue
FROM summary
ORDER BY event_count NULLS LAST;
