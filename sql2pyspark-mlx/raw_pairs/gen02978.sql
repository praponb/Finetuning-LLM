WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.created_at,
        t.coupon_codes,
        elem.product_id,
        elem.price,
        elem.promo
    FROM carts t
    LATERAL VIEW OUTER EXPLODE(t.items) x AS elem
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        price,
        promo,
        EXPLODE(coupon_codes) AS value
    FROM lines
),
metrics AS (
    -- Ranking and gap features
    SELECT
        price,
        promo,
        ROUND(price / SUM(price) OVER (PARTITION BY value, promo), 4) AS price_share,
        MIN(price) OVER (PARTITION BY value) AS value_min_price,
        ROUND(price / SUM(price) OVER (PARTITION BY value, promo), 4) AS price_share_2
    FROM flattened
    WHERE value != 'VIP'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        price,
        promo,
        price_share,
        value_min_price,
        price_share_2,
        LENGTH(promo) AS promo_length,
        price_share * price AS price_share_times_price
    FROM metrics
    WHERE price BETWEEN 80 AND 100
),
metrics_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price) AS price_percent_rank,
        RANK() OVER (ORDER BY price_share_2 DESC NULLS FIRST) AS price_share_2_rank,
        CUME_DIST() OVER (ORDER BY price DESC NULLS FIRST) AS price_cume_dist,
        RANK() OVER (ORDER BY price_share DESC NULLS LAST) AS price_share_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share_3
    FROM filtered
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        price,
        promo,
        price_share,
        value_min_price,
        price_share_2,
        price_percent_rank,
        price_cume_dist,
        price_share_rank,
        ROUND(price_share_rank * 100, 2) AS price_share_rank_adjusted,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM metrics_carts
    WHERE promo IS NOT NULL
),
filtered_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        price_share_2 - price_share_rank_adjusted AS price_share_2_minus_price_share_rank_adjusted,
        ROUND(value_min_price, 0) AS value_min_price_rounded,
        COALESCE(promo, 'DIRECT') AS promo_clean
    FROM filtered_carts
    WHERE promo IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    promo,
    measure,
    val
FROM (
    SELECT
        promo,
        price_share_2_minus_price_share_rank_adjusted,
        price_share,
        price
    FROM filtered_carts_2
) src
UNPIVOT (
    val FOR measure IN (price_share_2_minus_price_share_rank_adjusted AS PRICE_SHARE_2_MINUS_PRICE_SHARE_RANK_ADJUSTED_VAL, price_share AS PRICE_SHARE, price AS PRICE)
) long_rows;
