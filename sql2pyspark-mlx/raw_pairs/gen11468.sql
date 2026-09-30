WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) li AS li_item
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        promo,
        EXPLODE(coupon_codes) AS value
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        product_id,
        promo,
        value,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY value), 4) AS product_id_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share_2
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(product_id_share_2 AS INT) AS product_id_share_2_int,
        COALESCE(value, 'OTHER') AS value_clean
    FROM scored
    WHERE product_id_share > 0.34
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        product_id,
        promo,
        value,
        product_id_share_2,
        product_id_share_2_int,
        product_id_share_2 - product_id AS product_id_share_2_minus_product_id,
        COALESCE(value, 'UNKNOWN') AS value_filled
    FROM valid
    WHERE promo <> 'SPRING10'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY product_id_share_2_int ASC) AS product_id_share_2_int_dense_rank,
        MIN(product_id_share_2_int) OVER (PARTITION BY promo) AS promo_min_product_id_share_2_int,
        MIN(product_id) OVER w_ordered AS value_min_product_id
    FROM valid_carts
    WINDOW w_ordered AS (PARTITION BY value)
),
with_windows_carts AS (
    -- Ranking and gap features
    SELECT
        product_id_share_2_int,
        value_filled,
        value_min_product_id,
        PERCENT_RANK() OVER (ORDER BY product_id_share_2_minus_product_id DESC) AS product_id_share_2_minus_product_id_percent_rank,
        PERCENT_RANK() OVER (ORDER BY product_id_share_2 DESC) AS product_id_share_2_percent_rank,
        ROUND(product_id_share_2_int_dense_rank / SUM(product_id_share_2_int_dense_rank) OVER (PARTITION BY value), 4) AS product_id_share_2_int_dense_rank_share
    FROM with_windows
)
-- Ranking and gap features
SELECT
    product_id_share_2_minus_product_id_percent_rank,
    product_id_share_2_int_dense_rank_share,
    DENSE_RANK() OVER (ORDER BY product_id_share_2_minus_product_id_percent_rank DESC NULLS LAST) AS product_id_share_2_minus_product_id_percent_rank_dense_rank,
    DENSE_RANK() OVER (ORDER BY value_min_product_id ASC NULLS LAST) AS value_min_product_id_dense_rank,
    RANK() OVER (ORDER BY product_id_share_2_int_dense_rank_share DESC) AS product_id_share_2_int_dense_rank_share_rank
FROM with_windows_carts
