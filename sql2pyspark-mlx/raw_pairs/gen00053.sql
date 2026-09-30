WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines AS (
    SELECT
        customer_id,
        qty,
        price,
        promo,
        tag
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
base AS (
    SELECT
        *,
        UPPER(tag) AS tag_upper,
        ROUND(price, 2) AS price_rounded
    FROM lines
    WHERE customer_id IS NOT NULL
),
base_carts AS (
    SELECT
        *,
        CASE WHEN price_rounded >= 90 THEN 'High' WHEN price_rounded >= 40 THEN 'Medium' ELSE 'Low' END AS price_rounded_level,
        COALESCE(promo, 'N/A') AS resolved_promo
    FROM base
    WHERE tag IS NOT NULL
),
summary AS (
    SELECT
        CASE WHEN price_rounded >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_rounded_tier,
        promo,
        ROUND(SUM(qty) / NULLIF(COUNT(CASE WHEN tag = 'SPRING10' THEN 1 END), 0), 2) AS qty_per_spring10,
        SUM(qty) FILTER (WHERE tag = 'WELCOME5') AS welcome5_total_qty,
        SORT_ARRAY(COLLECT_SET(promo)) AS promo_list
    FROM base_carts
    GROUP BY price_rounded_tier, promo
    HAVING qty_per_spring10 >= 0
)
SELECT
    price_rounded_tier,
    promo,
    qty_per_spring10,
    welcome5_total_qty,
    promo_list
FROM summary;
