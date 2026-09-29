WITH flattened AS (
    SELECT
        id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            coupon_codes,
            EXPLODE(items) AS elem
        FROM carts
    ) li
),
flattened_carts AS (
    SELECT
        product_id,
        price,
        promo,
        value
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) e AS value
),
scored AS (
    SELECT
        product_id,
        value,
        COUNT(price) OVER w_running AS promo_count_price,
        SUM(product_id) OVER w_running AS promo_sum_product_id
    FROM flattened_carts
    WHERE promo IN ('WELCOME5', 'VIP')
    WINDOW w_running AS (PARTITION BY promo)
),
scored_carts AS (
    SELECT
        value,
        promo_sum_product_id,
        ROUND(promo_sum_product_id / SUM(promo_sum_product_id) OVER (PARTITION BY value), 4) AS promo_sum_product_id_share
    FROM scored
),
valid AS (
    SELECT
        *,
        CASE WHEN value = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10,
        COALESCE(value, 'OTHER') AS value_filled
    FROM scored_carts
    WHERE value IS DISTINCT FROM 'SPRING10'
),
scored_carts_2 AS (
    SELECT
        value,
        promo_sum_product_id_share,
        is_spring10,
        ROUND(promo_sum_product_id / SUM(promo_sum_product_id) OVER (PARTITION BY value), 4) AS promo_sum_product_id_share_2,
        ROUND(promo_sum_product_id / SUM(promo_sum_product_id) OVER (PARTITION BY value), 4) AS promo_sum_product_id_share_3
    FROM valid
),
valid_carts AS (
    SELECT
        value,
        promo_sum_product_id_share,
        promo_sum_product_id_share_2,
        promo_sum_product_id_share_3,
        LEAST(promo_sum_product_id_share_3, promo_sum_product_id_share_2) AS least_promo_sum_product_id_share_3_promo_sum_product_id_share_2,
        COALESCE(value, 'DIRECT') AS value_filled
    FROM scored_carts_2
    WHERE promo_sum_product_id_share_2 BETWEEN 0.24 AND 0.51
)
SELECT
    value,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN value = 'WELCOME5' THEN 1 END) AS welcome5_count,
    APPROX_PERCENTILE(promo_sum_product_id_share, 0.9) AS promo_sum_product_id_share_p90
FROM valid_carts
GROUP BY value
