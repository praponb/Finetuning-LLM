WITH flattened AS (
    SELECT
        c.id,
        c.coupon_codes,
        pos + 1 AS position,
        li_item.product_id,
        li_item.price,
        li_item.promo
    FROM carts c
    LATERAL VIEW POSEXPLODE(c.items) e AS pos, li_item
),
matched_keys AS (
    SELECT id
    FROM flattened
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE customer_id IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM flattened s
    JOIN matched_keys k ON s.id = k.id
),
with_windows AS (
    SELECT
        id,
        position,
        product_id,
        price,
        promo,
        MAX(product_id) OVER w_hist AS promo_max_product_id,
        STDDEV_SAMP(price) OVER w_hist AS promo_stddev_samp_price,
        DENSE_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank,
        RANK() OVER w_ordered AS price_rank
    FROM verified
    WHERE product_id BETWEEN 7 AND 8
    WINDOW w_hist AS (PARTITION BY promo), w_ordered AS (ORDER BY price DESC NULLS FIRST)
),
scored AS (
    SELECT
        id,
        position,
        product_id,
        promo_stddev_samp_price,
        product_id_dense_rank,
        price_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY position) AS position_cume_dist
    FROM with_windows
),
base AS (
    SELECT *, COALESCE(position, 0) AS position_filled
    FROM scored
    WHERE promo_stddev_samp_price BETWEEN 165 AND 174
),
base_carts AS (
    SELECT
        id,
        position,
        promo_stddev_samp_price,
        product_id_dense_rank,
        position_filled,
        CAST(product_id_dense_rank AS STRING) AS product_id_dense_rank_num,
        CASE WHEN promo_stddev_samp_price >= 264 THEN 'High' WHEN promo_stddev_samp_price >= 53 THEN 'Medium' ELSE 'Low' END AS promo_stddev_samp_price_level
    FROM base
    WHERE position_cume_dist <= 0.34
),
with_windows_carts AS (
    SELECT
        id,
        promo_stddev_samp_price,
        product_id_dense_rank,
        position_filled,
        promo_stddev_samp_price_level,
        PERCENT_RANK() OVER w_hist AS position_filled_percent_rank,
        DENSE_RANK() OVER w_running AS product_id_dense_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY position DESC NULLS FIRST) AS position_dense_rank
    FROM base_carts
    WINDOW w_hist AS (ORDER BY position_filled DESC NULLS LAST), w_running AS (ORDER BY product_id_dense_rank DESC NULLS LAST)
)
SELECT
    promo_stddev_samp_price,
    promo_stddev_samp_price_level,
    position_filled_percent_rank,
    position_dense_rank
FROM with_windows_carts
ORDER BY promo_stddev_samp_price;
