WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.qty,
        item.price
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS item
        FROM carts
    ) t
),
lines AS (
    SELECT qty, EXPLODE(coupon_codes) AS code
    FROM exploded
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY code ORDER BY qty DESC NULLS FIRST) AS qty_percent_rank,
        ROUND(qty / SUM(qty) OVER (PARTITION BY code), 4) AS qty_share,
        CUME_DIST() OVER (ORDER BY qty NULLS FIRST) AS qty_cume_dist
    FROM lines
)
SELECT
    qty,
    qty_percent_rank,
    qty_cume_dist
FROM enriched
