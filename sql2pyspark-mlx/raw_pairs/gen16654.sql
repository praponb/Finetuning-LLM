WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM (
        SELECT
            id,
            coupon_codes,
            EXPLODE(items) AS li_item
        FROM carts
    ) li
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        coupon_codes,
        product_id,
        price,
        promo,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS is_bundle
    FROM flattened
    WHERE promo IS DISTINCT FROM 'WELCOME5'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        price,
        promo,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        MAX(price) OVER (PARTITION BY promo) AS promo_max_price,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS FIRST) AS product_id_percent_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC) AS product_id_dense_rank
    FROM base
    WHERE SIZE(coupon_codes) >= 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN promo_max_price >= 270 THEN 'High' WHEN promo_max_price >= 74 THEN 'Medium' ELSE 'Low' END AS promo_max_price_level
    FROM scored
    WHERE product_id_dense_rank <= 5
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        promo,
        product_id_percent_rank,
        product_id_share
    FROM cleaned
) src
UNPIVOT (
    metric_value FOR metric IN (product_id_percent_rank, product_id_share)
)
