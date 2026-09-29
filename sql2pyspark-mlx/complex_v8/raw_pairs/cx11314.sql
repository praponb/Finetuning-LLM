WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
      AND end_date BETWEEN DATE '2025-09-27' AND DATE '2026-10-23'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        status_slug,
        MAX(premium) OVER (ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_max_2_prior,
        SIZE(COLLECT_SET(status) OVER w_product) AS distinct_status_in_product,
        ROUND(premium - AVG(premium) OVER w_product, 2) AS premium_vs_product_avg,
        SIZE(COLLECT_SET(product) OVER w_status) AS distinct_product_in_status,
        FIRST_VALUE(premium) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        LAST_VALUE(end_date) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        DENSE_RANK() OVER w_all_by_end_date_ties AS end_date_dense_rank
    FROM cleaned_policys
    WINDOW w_product AS (PARTITION BY product), w_status AS (PARTITION BY status), w_all_by_end_date_ties AS (ORDER BY end_date)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(premium_vs_product_avg, 0.0) AS premium_vs_product_avg_filled,
        ROUND(first_known_premium * 1.5, 2) AS first_known_premium_adj
    FROM sequenced
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        premium_max_2_prior,
        distinct_status_in_product,
        last_end_date,
        last_known_end_date,
        end_date_dense_rank,
        premium_vs_product_avg_filled,
        SUM(first_known_premium) OVER (PARTITION BY distinct_product_in_status) AS distinct_product_in_status_sum_first_known_premi,
        ROUND(AVG(first_known_premium) OVER w_distinct_product_in_status, 2) AS distinct_product_in_status_avg_first_known_premi,
        LAG(distinct_status_in_product) OVER (ORDER BY premium_vs_product_avg_filled DESC NULLS FIRST, id) AS prev_distinct_status_in_product
    FROM labeled
    WINDOW w_distinct_product_in_status AS (PARTITION BY distinct_product_in_status)
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    status,
    premium_max_2_prior,
    last_end_date,
    last_known_end_date,
    end_date_dense_rank,
    premium_vs_product_avg_filled,
    distinct_product_in_status_sum_first_known_premi,
    distinct_product_in_status_avg_first_known_premi,
    prev_distinct_status_in_product,
    ABS(end_date_dense_rank - 6) AS end_date_dense_rank_distance,
    SUBSTRING(TRIM(status), 1, 4) AS status_prefix
FROM with_trends
WHERE holder_id IS NOT NULL
ORDER BY premium DESC
