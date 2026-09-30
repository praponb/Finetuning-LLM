WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
    WHERE start_date < DATE '2025-03-21'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id) AS premium_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status)) AS distinct_holder_id_in_status,
        MIN(premium) OVER (PARTITION BY status) AS status_min_premium,
        COUNT(*) OVER w_status AS rows_in_status,
        SUM(premium) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_sum_6_prior,
        ROUND(CUME_DIST() OVER (ORDER BY premium, id), 3) AS premium_cume_dist,
        SUM(premium) OVER (PARTITION BY product ORDER BY start_date ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_sum_6_prior_2,
        NTILE(5) OVER w_holder_id_product_by_premium AS premium_quintile,
        FIRST_VALUE(premium) OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        MAX(premium) OVER (PARTITION BY product) AS product_max_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        RANK() OVER (ORDER BY premium) AS premium_rank_2
    FROM cleaned
    WINDOW w_status AS (PARTITION BY status), w_holder_id_product_by_premium AS (PARTITION BY holder_id, product ORDER BY premium DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product_ref,
        status_min_premium,
        rows_in_status,
        premium_sum_6_prior,
        premium_sum_6_prior_2,
        premium_quintile,
        first_premium,
        product_max_premium,
        second_premium,
        SUM(premium_sum_6_prior) OVER (PARTITION BY holder_id, distinct_holder_id_in_status) AS holder_id_distinct_holder_id_in_status_sum_premi,
        FIRST_VALUE(premium_sum_6_prior_2) IGNORE NULLS OVER (PARTITION BY premium_quintile ORDER BY distinct_holder_id_in_status, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_sum_6_prior_2,
        COUNT(*) OVER (PARTITION BY holder_id, distinct_holder_id_in_status) AS rows_in_holder_id_distinct_holder_id_in_status
    FROM sequenced
)
-- Final projection
SELECT
    id,
    holder_id,
    status_min_premium,
    rows_in_status,
    premium_sum_6_prior,
    premium_sum_6_prior_2,
    premium_quintile,
    first_premium,
    product_max_premium,
    second_premium,
    holder_id_distinct_holder_id_in_status_sum_premi,
    first_known_premium_sum_6_prior_2,
    rows_in_holder_id_distinct_holder_id_in_status,
    SUBSTRING(TRIM(product_ref), 1, 4) AS product_ref_prefix
FROM ranked
WHERE premium_sum_6_prior BETWEEN 3900.0 AND 12040.0
ORDER BY holder_id_distinct_holder_id_in_status_sum_premi, first_known_premium_sum_6_prior_2 DESC NULLS FIRST
