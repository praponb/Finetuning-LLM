WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        SUM(revenue) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        NTILE(3) OVER w_ordered AS revenue_bucket,
        CORR(revenue, units) OVER w_ordered_2 AS corr_revenue_units
    FROM store_sales
    WINDOW w_ordered AS (ORDER BY revenue, id), w_ordered_2 AS (PARTITION BY product_id)
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue_bucket DESC NULLS LAST) AS revenue_bucket_dense_rank,
        CUME_DIST() OVER (ORDER BY corr_revenue_units NULLS FIRST) AS corr_revenue_units_cume_dist,
        ROUND(revenue_bucket / SUM(revenue_bucket) OVER (PARTITION BY product_id), 4) AS revenue_bucket_share,
        ROUND(running_revenue / SUM(running_revenue) OVER (PARTITION BY product_id, store_id), 4) AS running_revenue_share,
        LEAD(revenue, 1, 0) OVER (PARTITION BY store_id, product_id ORDER BY units, id) AS next_revenue
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY corr_revenue_units_cume_dist, id) AS corr_revenue_units_cume_dist_bucket
    FROM enriched_store_sales
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        running_revenue,
        revenue_bucket,
        corr_revenue_units,
        corr_revenue_units_cume_dist,
        revenue_bucket_share,
        running_revenue_share,
        next_revenue,
        revenue_share,
        corr_revenue_units_cume_dist_bucket,
        DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label,
        revenue_bucket_dense_rank * corr_revenue_units_cume_dist AS revenue_bucket_dense_rank_times_corr_revenue_units_cume_dist
    FROM with_windows
    WHERE sale_date > DATE '2024-05-03'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        running_revenue,
        revenue_bucket,
        corr_revenue_units,
        corr_revenue_units_cume_dist,
        revenue_bucket_share,
        revenue_share,
        corr_revenue_units_cume_dist_bucket,
        revenue_bucket_dense_rank_times_corr_revenue_units_cume_dist,
        ROUND(corr_revenue_units_cume_dist * 0.9, 2) AS corr_revenue_units_cume_dist_adjusted
    FROM valid
    WHERE next_revenue < 2100
),
with_windows_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        running_revenue,
        revenue_bucket,
        corr_revenue_units,
        corr_revenue_units_cume_dist,
        revenue_bucket_share,
        revenue_share,
        corr_revenue_units_cume_dist_bucket,
        revenue_bucket_dense_rank_times_corr_revenue_units_cume_dist,
        corr_revenue_units_cume_dist_adjusted,
        LAG(corr_revenue_units_cume_dist_adjusted, 1) OVER (PARTITION BY store_id, product_id ORDER BY revenue_bucket_share, id) AS prev_corr_revenue_units_cume_dist_adjusted,
        LEAD(corr_revenue_units_cume_dist_adjusted, 1) OVER (PARTITION BY store_id ORDER BY revenue_bucket, id) AS next_corr_revenue_units_cume_dist_adjusted
    FROM base
),
enriched_store_sales_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(corr_revenue_units) OVER w_hist AS product_id_sum_corr_revenue_units,
        LEAD(corr_revenue_units_cume_dist_adjusted, 1) OVER (PARTITION BY store_id ORDER BY revenue_bucket_share, id) AS next_corr_revenue_units_cume_dist_adjusted_2,
        AVG(corr_revenue_units_cume_dist) OVER w_grp AS corr_revenue_units_cume_dist_avg_2_prior,
        LAST_VALUE(corr_revenue_units_cume_dist_adjusted, TRUE) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_corr_revenue_units_cume_dist_adjusted,
        NTILE(10) OVER w_recent AS revenue_bucket_share_bucket
    FROM with_windows_store_sales
    WINDOW w_hist AS (PARTITION BY product_id, store_id), w_grp AS (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_recent AS (PARTITION BY product_id ORDER BY revenue_bucket_share, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN corr_revenue_units_cume_dist_avg_2_prior >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_revenue_units_cume_dist_avg_2_prior_tier,
    product_id,
    CASE WHEN revenue_share >= 0.8 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_share_tier,
    MAX(corr_revenue_units) AS max_corr_revenue_units,
    COUNT(*) AS row_count,
    ROUND(AVG(corr_revenue_units_cume_dist_avg_2_prior), 2) AS avg_corr_revenue_units_cume_dist_avg_2_prior,
    RANK() OVER (PARTITION BY CASE WHEN corr_revenue_units_cume_dist_avg_2_prior >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS row_count_rank
FROM enriched_store_sales_2
GROUP BY corr_revenue_units_cume_dist_avg_2_prior_tier, product_id, revenue_share_tier
HAVING avg_corr_revenue_units_cume_dist_avg_2_prior > 0
ORDER BY corr_revenue_units_cume_dist_avg_2_prior_tier
