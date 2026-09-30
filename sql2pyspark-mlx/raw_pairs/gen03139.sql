WITH exploded AS (
    SELECT
        base.id,
        base.customer_id,
        base.coupon_codes,
        entry.product_id,
        entry.qty,
        entry.price,
        entry.promo
    FROM carts base
    LATERAL VIEW OUTER EXPLODE(base.items) e AS entry
),
lines AS (
    SELECT
        qty,
        price,
        promo,
        code
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) t AS code
),
base AS (
    SELECT
        *,
        COALESCE(qty, 0) AS qty_filled,
        COALESCE(code, 'UNKNOWN') AS code_filled
    FROM lines
    WHERE code NOT IN ('SPRING10', 'WELCOME5', 'BUNDLE')
),
metrics AS (
    SELECT
        price,
        promo,
        qty_filled,
        ROUND(qty / SUM(qty) OVER (PARTITION BY code), 4) AS qty_share
    FROM base
)
SELECT
    CASE WHEN price >= 264 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    CASE WHEN qty_filled >= 61 THEN 'LARGE' ELSE 'SMALL' END AS qty_filled_tier,
    COUNT(DISTINCT promo) AS distinct_promos,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    PERCENTILE_APPROX(qty_share, 0.95) AS qty_share_p95
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN price >= 264 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty_filled >= 61 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN price >= 264 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY qty_share_p95
