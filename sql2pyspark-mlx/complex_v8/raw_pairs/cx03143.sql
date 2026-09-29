WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE status IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status, product), 0), 4) AS premium_share_of_status_product,
        MAX(premium) OVER (PARTITION BY status, product) AS status_product_max_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        LEAD(premium) OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id) AS next_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS premium_vs_product_holder_id_avg,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_holder_id, 0), 4) AS premium_share_of_product_holder_id,
        ROUND(AVG(premium) OVER w_product_holder_id, 2) AS product_holder_id_avg_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY premium ASC), 3) AS premium_cume_dist,
        ROUND(premium - AVG(premium) OVER w_status_product, 2) AS premium_vs_status_product_avg,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, product)) AS distinct_holder_id_in_status_product,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY premium DESC, id) AS seq_no
    FROM scoped_policys
    WINDOW w_product_holder_id AS (PARTITION BY product, holder_id), w_status_product AS (PARTITION BY status, product)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE seq_no = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    status_slug,
    premium_share_of_status_product,
    status_product_max_premium,
    second_premium,
    premium_vs_product_holder_id_avg,
    premium_share_of_product_holder_id,
    premium_cume_dist,
    premium_vs_status_product_avg,
    distinct_holder_id_in_status_product,
    first_known_premium
FROM top_rows
WHERE holder_id IS NOT NULL
ORDER BY product NULLS LAST, id
LIMIT 5
