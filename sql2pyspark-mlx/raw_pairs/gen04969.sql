WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t0.id,
        t0.coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts t0
    LATERAL VIEW EXPLODE(t0.items) t AS li_item
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        qty,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM exploded
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(qty, 0) AS qty_filled
    FROM lines
    WHERE promo IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        qty,
        promo,
        qty_filled,
        AVG(price) OVER (PARTITION BY promo) AS promo_avg_price,
        RANK() OVER (PARTITION BY promo ORDER BY qty_filled DESC NULLS LAST) AS qty_filled_rank
    FROM cleaned
    WHERE price <= 200
),
cleaned_carts AS (
    -- Basic cleansing and defaults
    SELECT
        qty,
        promo,
        promo_avg_price,
        qty_filled_rank,
        LEAST(qty_filled_rank, qty) AS least_qty_filled_rank_qty,
        promo_avg_price / NULLIF(qty_filled, 0) AS promo_avg_price_per_qty_filled
    FROM metrics
    WHERE promo IS NOT NULL
)
SELECT
    promo,
    promo_avg_price,
    qty_filled_rank,
    promo_avg_price_per_qty_filled,
    CASE WHEN promo RLIKE '^(SPRING10|BUNDLE)$' THEN 'GROUP_B' WHEN promo RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS promo_zone
FROM cleaned_carts
