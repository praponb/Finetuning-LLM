WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock <= s.opened_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(sqft) OVER (PARTITION BY region) region_min_sqft,
        CUME_DIST() OVER w_hist sqft_cume_dist,
        SUM(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft
    FROM not_flagged
    WINDOW w_hist AS (PARTITION BY city ORDER BY sqft DESC)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY region_min_sqft DESC, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        region_min_sqft,
        sqft_cume_dist,
        running_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY region_min_sqft, id) next_sqft,
        SUM(region_min_sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_region_min_sqft,
        RANK() OVER (ORDER BY sqft_cume_dist DESC) sqft_cume_dist_rank
    FROM ranked
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.order_amount,
        x.discount
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.amount AS order_amount, c.discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        region,
        APPROX_PERCENTILE(sqft_cume_dist_rank, 0.5) AS sqft_cume_dist_rank_p50,
        SUM(sqft) AS total_sqft,
        SIZE(COLLECT_SET(region)) AS num_regions,
        ROUND(SUM(sqft_cume_dist) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS sqft_cume_dist_per_london,
        COUNT(*) AS num_rows,
        COUNT(*) AS row_count
    FROM with_last
    GROUP BY region
)
SELECT
    region,
    total_sqft,
    num_regions,
    sqft_cume_dist_per_london,
    num_rows,
    row_count
FROM grouped
ORDER BY num_rows DESC;
