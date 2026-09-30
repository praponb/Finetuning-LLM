WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS line
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    promo,
    CUME_DIST() OVER (PARTITION BY promo ORDER BY product_id DESC) AS product_id_cume_dist,
    COUNT(product_id) OVER w_part AS created_at_period_count_product_id
FROM lines
WINDOW w_part AS (PARTITION BY DATE_TRUNC('WEEK', created_at), promo)
