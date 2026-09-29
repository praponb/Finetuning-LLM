WITH prepared AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status = 'EXPIRED'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY product ORDER BY end_date ASC NULLS LAST), 3) AS end_date_cume_dist,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(AVG(premium) OVER (PARTITION BY product), 2) AS product_avg_premium,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        MAX(premium) OVER (ORDER BY premium ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_max_1_prior,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        SUM(premium) OVER (PARTITION BY product ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(CUME_DIST() OVER w_all_by_end_date, 3) AS end_date_cume_dist_2,
        ROUND(AVG(premium) OVER (ORDER BY premium ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        RANK() OVER w_all_by_premium_ties AS premium_rank
    FROM prepared
    WINDOW w_all_by_end_date AS (ORDER BY end_date ASC, id), w_all_by_premium_ties AS (ORDER BY premium ASC), w_product AS (PARTITION BY product)
),
scored AS (
    -- Add calculated columns
    SELECT *, CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM windowed
    WHERE premium > 1132.1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        CASE WHEN running_premium >= 32570.0 THEN 'HIGH' WHEN running_premium >= 15140.0 THEN 'MEDIUM' ELSE 'LOW' END AS running_premium_tier,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        ROUND(SUM(premium_avg_2) / NULLIF(SUM(end_date_cume_dist), 0), 3) AS premium_avg_2_to_end_date_cume_dist_ratio,
        SUM(CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END) AS n_status_is_not_null,
        SUM(CASE WHEN product = 'TRAVEL' THEN 1 ELSE 0 END) AS n_product_travel,
        PERCENTILE_APPROX(product_avg_premium, 0.5) AS median_product_avg_premium,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM scored
    GROUP BY status, CASE WHEN running_premium >= 32570.0 THEN 'HIGH' WHEN running_premium >= 15140.0 THEN 'MEDIUM' ELSE 'LOW' END, DATE_FORMAT(end_date, 'yyyy-MM')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        status,
        running_premium_tier,
        end_date_ym,
        premium_avg_2_to_end_date_cume_dist_ratio,
        n_status_is_not_null,
        n_product_travel,
        product_list,
        ROUND(CUME_DIST() OVER (PARTITION BY status, running_premium_tier ORDER BY n_status_is_not_null NULLS LAST, status, running_premium_tier, end_date_ym), 3) AS n_status_is_not_null_cume_dist,
        SUM(median_product_avg_premium) OVER (ORDER BY median_product_avg_premium, status, running_premium_tier, end_date_ym ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS median_product_avg_premium_sum_4,
        ROUND(n_product_travel / NULLIF(SUM(n_product_travel) OVER (PARTITION BY n_product_travel, n_status_is_not_null), 0), 4) AS n_product_travel_share_of_n_product_travel_n_sta,
        DENSE_RANK() OVER w_all_by_median_product_avg_pr_2 AS median_product_avg_premium_dense_rank
    FROM per_group
    WINDOW w_all_by_median_product_avg_pr_2 AS (ORDER BY median_product_avg_premium, status, running_premium_tier, end_date_ym)
)
SELECT
    status,
    running_premium_tier,
    end_date_ym,
    premium_avg_2_to_end_date_cume_dist_ratio,
    n_status_is_not_null,
    n_product_travel,
    n_status_is_not_null_cume_dist,
    median_product_avg_premium_sum_4,
    n_product_travel_share_of_n_product_travel_n_sta,
    median_product_avg_premium_dense_rank,
    INITCAP(TRIM(running_premium_tier)) AS running_premium_tier_clean
FROM sequenced
ORDER BY running_premium_tier_clean DESC
