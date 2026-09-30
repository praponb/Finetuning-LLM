WITH lines AS (
    SELECT
        c.id,
        c.created_at,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts c
    LATERAL VIEW OUTER EXPLODE(c.items) t AS item
)
SELECT
    promo,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    MIN(created_at) AS first_seen,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes_2
FROM lines
GROUP BY promo
HAVING span_minutes >= 0
