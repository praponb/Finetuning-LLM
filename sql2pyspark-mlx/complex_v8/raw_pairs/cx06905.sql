WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status IN ('ACTIVE', 'LAPSED', 'EXPIRED')
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.status_slug,
        s.premium_filled,
        p.region AS holder_region
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER w_holder_id, 2) AS premium_vs_holder_id_avg,
        NTH_VALUE(premium_filled, 2) OVER (ORDER BY premium_filled NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_filled,
        MAX(premium) OVER (PARTITION BY product) AS product_max_premium,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, status ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        MAX(premium) OVER (PARTITION BY holder_region, holder_id ORDER BY premium ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_max_3,
        LAG(premium) OVER (PARTITION BY holder_id, status ORDER BY premium ASC, id) AS prev_premium,
        LAG(premium) OVER w_product_holder_id_by_premium AS prev_premium_2,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium_filled DESC), 3) AS premium_filled_cume_dist,
        ROUND(AVG(premium) OVER (ORDER BY premium_filled NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        DENSE_RANK() OVER w_product_by_premium_ties AS premium_dense_rank
    FROM joined_holder
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_product_holder_id_by_premium AS (PARTITION BY product, holder_id ORDER BY premium_filled DESC, id), w_product_by_premium_ties AS (PARTITION BY product ORDER BY premium)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE premium_dense_rank <= 5
      AND premium_filled_cume_dist >= 0.1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY premium) AS premium_dense_rank_2,
        LAG(premium_avg_7, 3) OVER (PARTITION BY status, holder_id ORDER BY premium_max_3, id) AS prev3_premium_avg_7,
        LEAD(premium_vs_holder_id_avg, 2) OVER w_status_holder_id_by_premium AS next2_premium_vs_holder_id_avg,
        MIN(premium_avg_7) OVER (PARTITION BY premium_dense_rank) AS premium_dense_rank_min_premium_avg_7,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium, id) AS rn,
        MIN(premium_vs_holder_id_avg) OVER w_premium_dense_rank AS premium_dense_rank_min_premium_vs_holder_id_avg,
        ROUND(AVG(premium_dense_rank) OVER (PARTITION BY status, holder_id ORDER BY premium_max_3, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_dense_rank_avg_2
    FROM first_rows
    WINDOW w_status_holder_id_by_premium AS (PARTITION BY status, holder_id ORDER BY premium_max_3, id), w_premium_dense_rank AS (PARTITION BY premium_dense_rank)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    status,
    status_slug,
    premium_filled,
    holder_region,
    premium_vs_holder_id_avg,
    product_max_premium,
    premium_avg_7,
    premium_max_3,
    prev_premium,
    prev_premium_2,
    premium_filled_cume_dist,
    premium_avg_2,
    premium_dense_rank,
    premium_dense_rank_2,
    prev3_premium_avg_7,
    premium_dense_rank_min_premium_avg_7,
    rn,
    premium_dense_rank_min_premium_vs_holder_id_avg,
    premium_dense_rank_avg_2,
    ROUND(premium * 1.5, 2) AS premium_adj,
    CASE WHEN holder_region IN ('West', 'South') THEN 'group_a' WHEN holder_region = 'Central' THEN 'group_b' ELSE 'other' END AS holder_region_group
FROM ranked
WHERE premium_vs_holder_id_avg < 2000.0
ORDER BY prev_premium DESC
