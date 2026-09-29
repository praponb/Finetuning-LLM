WITH flattened AS (
    SELECT
        t.id,
        t.created_at,
        t.coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) x AS elem
),
exploded AS (
    SELECT
        created_at,
        qty,
        price,
        promo,
        element
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS element
),
filtered AS (
    SELECT
        qty,
        element,
        ROUND(qty, 1) AS qty_rounded,
        CASE WHEN qty >= 5 THEN 'High' WHEN qty >= 2 THEN 'Medium' ELSE 'Low' END AS qty_tier
    FROM exploded
    WHERE promo IS NOT NULL
      AND promo = 'VIP'
),
cleaned AS (
    SELECT *, INITCAP(TRIM(element)) AS element_title
    FROM filtered
    WHERE element = 'VIP'
)
SELECT
    qty,
    element,
    qty_rounded,
    qty_tier,
    element_title
FROM cleaned;
