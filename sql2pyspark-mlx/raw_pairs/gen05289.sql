WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS li_item
        FROM carts
    ) li
),
lines_carts AS (
    SELECT
        id,
        created_at,
        qty,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM lines
),
filtered AS (
    SELECT
        *,
        INITCAP(TRIM(element)) AS element_title,
        COALESCE(element, 'OTHER') AS element_filled
    FROM lines_carts
    WHERE created_at >= TIMESTAMP '2025-03-01 20:00:00'
      AND element IS NOT NULL
),
metrics AS (
    SELECT
        id,
        qty,
        promo,
        SUM(qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_sum_30m
    FROM filtered
    WHERE created_at < '2025-02-20'
)
SELECT
    CASE WHEN qty_sum_30m >= 24 THEN 'HIGH' ELSE 'LOW' END AS qty_sum_30m_tier,
    CASE WHEN qty >= 4 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    COUNT_IF(promo = 'BUNDLE') AS bundle_events,
    ROUND(AVG(qty), 2) AS avg_qty
FROM metrics
GROUP BY CASE WHEN qty_sum_30m >= 24 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty >= 4 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_qty DESC;
