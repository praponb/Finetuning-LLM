WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM policies
    WHERE premium < 3465.4
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        start_date,
        premium_adj,
        COUNT(*) OVER (PARTITION BY holder_id, status) AS rows_in_holder_id_status,
        NTILE(4) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY premium, id) AS premium_quartile,
        SUM(premium_adj) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_adj,
        ROUND(CUME_DIST() OVER w_product_status_by_premium_ad, 3) AS premium_adj_cume_dist,
        ROUND(AVG(premium_adj) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_adj_avg_6_prior,
        premium_adj - LAG(premium_adj) OVER w_product_start_date_year_by_e AS premium_adj_change,
        SIZE(COLLECT_SET(product) OVER w_holder_id_status) AS distinct_product_in_holder_id_status,
        ROUND(AVG(premium_adj) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_adj_avg_6_prior_2,
        NTILE(10) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id) AS start_date_decile,
        FIRST_VALUE(start_date) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        NTILE(10) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY premium, id) AS premium_decile,
        NTILE(3) OVER (PARTITION BY product, status ORDER BY premium_adj ASC, id) AS premium_adj_tercile
    FROM filtered
    WINDOW w_product_status_by_premium_ad AS (PARTITION BY product, status ORDER BY premium_adj ASC), w_product_start_date_year_by_e AS (PARTITION BY product, YEAR(start_date) ORDER BY end_date, id), w_holder_id_status AS (PARTITION BY holder_id, status)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        premium,
        premium_adj,
        rows_in_holder_id_status,
        premium_quartile,
        premium_adj_avg_6_prior,
        premium_adj_change,
        premium_adj_avg_6_prior_2,
        start_date_decile,
        premium_decile,
        DAYOFWEEK(first_start_date) AS first_start_date_dayofweek,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        COALESCE(premium_decile, 0) AS premium_decile_filled
    FROM metrics
    WHERE premium_decile >= 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        premium,
        premium_quartile,
        premium_adj_avg_6_prior,
        premium_adj_avg_6_prior_2,
        start_date_decile,
        premium_decile,
        RANK() OVER w_premium_decile_filled_premiu AS premium_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY premium_decile_filled, premium_quartile ORDER BY premium, id), 3) AS premium_cume_dist,
        ROUND(premium_adj_avg_6_prior_2 - AVG(premium_adj_avg_6_prior_2) OVER (PARTITION BY start_date_decile), 2) AS premium_adj_avg_6_prior_2_vs_start_date_decile_a,
        LAST_VALUE(premium_quartile) IGNORE NULLS OVER (PARTITION BY premium_decile_filled, premium_quartile ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_quartile,
        SUM(premium_quartile) OVER (PARTITION BY start_date_decile) AS start_date_decile_sum_premium_quartile,
        SUM(premium) OVER (ORDER BY premium_decile, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior
    FROM derived
    WINDOW w_premium_decile_filled_premiu AS (PARTITION BY premium_decile_filled, premium_quartile ORDER BY premium, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        CASE WHEN premium_sum_3_prior >= 10560.0 THEN 'A' WHEN premium_sum_3_prior >= 4640.0 THEN 'B' ELSE 'C' END AS premium_sum_3_prior_tier,
        premium_rank,
        premium_decile,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(premium_quartile, 0.5) AS median_premium_quartile,
        ROUND(SUM(start_date_decile_sum_premium_quartile) / NULLIF(SUM(premium), 0), 3) AS start_date_decile_sum_premium_quartile_to_premiu,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(premium_cume_dist) / NULLIF(SUM(last_known_premium_quartile), 0), 3) AS premium_cume_dist_to_last_known_premium_quartile,
        MAX(premium_cume_dist) AS max_premium_cume_dist
    FROM with_trends
    GROUP BY CASE WHEN premium_sum_3_prior >= 10560.0 THEN 'A' WHEN premium_sum_3_prior >= 4640.0 THEN 'B' ELSE 'C' END, premium_rank, premium_decile
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY median_premium_quartile), 2) AS distinct_ids_vs_median_premium_quartile_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_ids, row_count ORDER BY median_premium_quartile, premium_sum_3_prior_tier, premium_rank, premium_decile), 3) AS median_premium_quartile_percent_rank,
        LEAD(median_premium_quartile) OVER (PARTITION BY distinct_ids, row_count ORDER BY median_premium_quartile, premium_sum_3_prior_tier, premium_rank, premium_decile) AS next_median_premium_quartile,
        NTH_VALUE(max_premium_cume_dist, 2) OVER (PARTITION BY premium_sum_3_prior_tier ORDER BY distinct_ids NULLS LAST, premium_sum_3_prior_tier, premium_rank, premium_decile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_premium_cume_dist
    FROM per_group
)
-- Final projection
SELECT
    premium_sum_3_prior_tier,
    premium_rank,
    premium_decile,
    row_count,
    median_premium_quartile,
    premium_cume_dist_to_last_known_premium_quartile,
    distinct_ids_vs_median_premium_quartile_avg,
    median_premium_quartile_percent_rank,
    next_median_premium_quartile,
    second_max_premium_cume_dist,
    FLOOR(row_count / 2) * 2 AS row_count_bucket,
    UPPER(TRIM(premium_sum_3_prior_tier)) AS premium_sum_3_prior_tier_clean
FROM with_windows
ORDER BY row_count NULLS LAST
