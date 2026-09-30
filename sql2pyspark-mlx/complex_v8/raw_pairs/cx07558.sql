WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ABS(premium - 2111.3) AS premium_distance,
        ABS(premium - 3465.4) AS premium_distance_2
    FROM policies
    WHERE holder_id >= 11
      AND status = 'ACTIVE'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(premium_distance_2) OVER (PARTITION BY holder_id, product ORDER BY premium_distance DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_2_avg_3_prior,
        premium_distance_2 - LAG(premium_distance_2) OVER (ORDER BY premium, id) AS premium_distance_2_change,
        SUM(premium_distance_2) OVER (PARTITION BY product, holder_id ORDER BY premium_distance_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_distance_2,
        FIRST_VALUE(premium) IGNORE NULLS OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        RANK() OVER (ORDER BY start_date ASC, id) AS start_date_rank,
        ROUND(AVG(premium) OVER w_status, 2) AS status_avg_premium,
        LAG(premium) OVER w_holder_id_product_by_premium_2 AS prev_premium,
        ROUND(premium_distance_2 / NULLIF(SUM(premium_distance_2) OVER (PARTITION BY holder_id, product), 0), 4) AS premium_distance_2_share_of_holder_id_product,
        ROUND(AVG(premium_distance) OVER (ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_avg_6_prior,
        ROW_NUMBER() OVER w_holder_id_product_by_premium_2 AS rn,
        premium_distance_2 - LAG(premium_distance_2) OVER (PARTITION BY holder_id, product ORDER BY premium_distance DESC, id) AS premium_distance_2_change_2,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status
    FROM base
    WINDOW w_status AS (PARTITION BY status), w_holder_id_product_by_premium_2 AS (PARTITION BY holder_id, product ORDER BY premium_distance DESC, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY rn, start_date_rank)) AS distinct_holder_id_in_rn_start_date_rank,
        NTILE(5) OVER (ORDER BY premium_distance_2_change_2 DESC NULLS FIRST, id) AS premium_distance_2_change_2_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY rn ORDER BY rows_in_status NULLS LAST), 3) AS rows_in_status_cume_dist,
        LAST_VALUE(premium_distance) IGNORE NULLS OVER (ORDER BY premium_distance_2_change_2 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_distance,
        SIZE(COLLECT_SET(status) OVER w_rn_start_date_rank) AS distinct_status_in_rn_start_date_rank,
        DENSE_RANK() OVER (ORDER BY premium_distance_2_change_2 DESC NULLS FIRST) AS premium_distance_2_change_2_dense_rank
    FROM sequenced
    WINDOW w_rn_start_date_rank AS (PARTITION BY rn, start_date_rank)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status,
    premium_distance,
    premium_distance_2,
    premium_distance_2_avg_3_prior,
    premium_distance_2_change,
    running_premium_distance_2,
    start_date_rank,
    premium_distance_2_share_of_holder_id_product,
    premium_distance_avg_6_prior,
    rn,
    premium_distance_2_change_2,
    rows_in_status,
    distinct_holder_id_in_rn_start_date_rank,
    premium_distance_2_change_2_quintile,
    distinct_status_in_rn_start_date_rank,
    premium_distance_2_change_2_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
    CASE WHEN first_known_premium >= 2945.9 THEN 'large' WHEN first_known_premium >= 1541.8 THEN 'medium' ELSE 'small' END AS first_known_premium_tier
FROM sequenced_2
ORDER BY product DESC
