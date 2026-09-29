WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) e
),
lines AS (
    SELECT
        qty,
        price,
        promo,
        element
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
),
metrics AS (
    SELECT *, ROUND(qty / SUM(qty) OVER (PARTITION BY element, promo), 4) AS qty_share
    FROM lines
),
scored AS (
    SELECT
        *,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share_2,
        PERCENT_RANK() OVER w_running AS price_percent_rank,
        CUME_DIST() OVER w_ordered AS qty_share_cume_dist
    FROM metrics
    WINDOW w_running AS (ORDER BY price DESC NULLS LAST), w_ordered AS (ORDER BY qty_share DESC NULLS FIRST)
),
scored_carts AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY element ORDER BY qty_share_cume_dist) AS qty_share_cume_dist_cume_dist
    FROM scored
    WHERE element IN ('WELCOME5', 'SPRING10')
)
SELECT
    qty,
    price,
    promo,
    qty_share,
    price_percent_rank,
    qty_share_cume_dist,
    qty_share_cume_dist_cume_dist,
    CASE WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_A' WHEN promo RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS promo_zone
FROM scored_carts;
