WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        LOWER(region) AS region_lower,
        CAST(sqft AS STRING) AS sqft_str,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
with_last AS (
    SELECT s.*, x.inventory_last_restock
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.customer_id IS NULL
        )
),
with_windows AS (
    SELECT
        *,
        LEAD(sqft, 1) OVER w_running AS next_sqft,
        AVG(sqft) OVER w_running_2 AS sqft_avg_6_prior,
        AVG(sqft) OVER (PARTITION BY region ORDER BY inventory_last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3
    FROM not_flagged
    WINDOW w_running AS (PARTITION BY city ORDER BY sqft, id), w_running_2 AS (PARTITION BY DATE_TRUNC('month', inventory_last_restock), city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
scored AS (
    SELECT
        *,
        MAX(sqft) OVER w_grp AS last7_max_sqft,
        SUM(sqft) OVER w_grp AS last7_sum_sqft,
        AVG(sqft) OVER w_grp AS last7_avg_sqft,
        MIN(sqft_avg_6_prior) OVER w_ordered AS inventory_last_restock_period_min_sqft_avg_6_prior
    FROM with_windows
    WHERE city <> 'Chiang Mai'
    WINDOW w_grp AS (PARTITION BY region ORDER BY sqft_avg_6_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY DATE_TRUNC('YEAR', inventory_last_restock), city)
),
metrics AS (
    SELECT
        *,
        AVG(sqft_avg_6_prior) OVER (PARTITION BY YEAR(inventory_last_restock), city ORDER BY sqft_avg_3, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior_avg_6_prior,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        MAX(last7_avg_sqft) OVER (PARTITION BY DATE_TRUNC('YEAR', inventory_last_restock)) AS inventory_last_restock_period_max_last7_avg_sqft,
        SUM(last7_sum_sqft) OVER (PARTITION BY region, city ORDER BY sqft_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_sum_sqft,
        RANK() OVER (PARTITION BY region ORDER BY last7_avg_sqft NULLS LAST) AS last7_avg_sqft_rank
    FROM scored
),
kept_keys AS (
    SELECT id, city
    FROM metrics
    EXCEPT DISTINCT
    SELECT id, city
    FROM stores
    WHERE city IN ('Bangkok', 'Berlin', 'Singapore')
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
stats AS (
    SELECT
        CASE WHEN sqft >= 17393 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
        CASE WHEN last7_avg_sqft_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_avg_sqft_rank_tier,
        DATE_TRUNC('MONTH', inventory_last_restock) AS activity_month,
        SIZE(COLLECT_SET(region)) AS num_regions,
        COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows
    FROM verified
    GROUP BY CASE WHEN sqft >= 17393 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last7_avg_sqft_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', inventory_last_restock)
)
SELECT
    sqft_tier,
    last7_avg_sqft_rank_tier,
    activity_month,
    num_regions,
    bangkok_rows
FROM stats
ORDER BY sqft_tier DESC, last7_avg_sqft_rank_tier DESC;
