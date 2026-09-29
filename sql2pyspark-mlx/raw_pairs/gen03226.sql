WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos + 1 AS position,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) t AS pos, line
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    coupon_codes,
    product_id,
    qty,
    promo,
    DENSE_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC) AS product_id_dense_rank
FROM flattened
WHERE price >= 240;
