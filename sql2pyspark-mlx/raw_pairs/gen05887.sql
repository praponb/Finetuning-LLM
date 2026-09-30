WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        coupon_codes,
        product_id,
        qty,
        promo,
        LEAST(product_id, price) AS least_product_id_price,
        ROUND(qty * 1.1, 2) AS qty_scaled,
        COALESCE(promo, 'NONE') AS resolved_promo
    FROM exploded
    WHERE qty >= 3
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(promo) AS promo_lower,
        COALESCE(promo, 'NONE') AS resolved_promo_2
    FROM base
    WHERE qty_scaled > 67
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_percent_rank,
        SUM(qty_scaled) OVER (PARTITION BY promo) AS promo_sum_qty_scaled,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY qty) AS qty_percent_rank_2
    FROM valid
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(least_product_id_price) AS least_product_id_price_whole
    FROM with_windows
    WHERE promo IS NOT NULL
      AND promo = 'WELCOME5'
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    SUM(CASE WHEN promo = 'BUNDLE' THEN qty_percent_rank_2 ELSE 0 END) AS bundle_qty_percent_rank_2,
    COUNT(*) FILTER (WHERE promo = 'VIP') AS vip_rows,
    COUNT(*) FILTER (WHERE promo = 'VIP') AS vip_rows_2,
    PERCENTILE_APPROX(least_product_id_price, ARRAY(0.25, 0.5, 0.75)) AS least_product_id_price_quartiles,
    COUNT(CASE WHEN promo = 'VIP' THEN 1 END) AS vip_count
FROM valid_carts
GROUP BY product_id
ORDER BY bundle_qty_percent_rank_2 DESC;
