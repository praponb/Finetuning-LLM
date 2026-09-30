WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        item.qty,
        item.price,
        item.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, item
),
cleaned AS (
    SELECT
        *,
        CASE WHEN price >= 206 THEN 'High' WHEN price >= 41 THEN 'Medium' ELSE 'Low' END AS price_tier,
        HOUR(created_at) AS created_at_hour,
        COALESCE(promo, 'NONE') AS promo_filled
    FROM flattened
    WHERE pos <= 3
),
cleaned_carts AS (
    SELECT
        *,
        COALESCE(promo, 'unassigned') AS promo_filled_2,
        CASE WHEN created_at_hour >= 74 THEN 'High' WHEN created_at_hour >= 14 THEN 'Medium' ELSE 'Low' END AS created_at_hour_band
    FROM cleaned
    WHERE created_at >= DATE '2025-03-01'
),
cleaned_carts_2 AS (
    SELECT
        created_at,
        pos,
        qty,
        price,
        promo,
        price_tier,
        created_at_hour,
        promo_filled,
        promo_filled_2,
        created_at_hour_band,
        SIZE(coupon_codes) AS coupon_codes_count,
        GREATEST(qty, price) AS greatest_qty_price,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM cleaned_carts
    WHERE promo IS NOT NULL
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    SUM(CASE WHEN promo = 'BUNDLE' THEN greatest_qty_price ELSE 0 END) AS bundle_greatest_qty_price,
    ROUND(AVG(coupon_codes_count), 2) AS avg_coupon_codes_count
FROM cleaned_carts_2
GROUP BY 1
HAVING ROUND(AVG(coupon_codes_count), 2) < 1000000
ORDER BY avg_coupon_codes_count DESC NULLS FIRST
