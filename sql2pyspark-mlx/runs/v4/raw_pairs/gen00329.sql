WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS line
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY qty DESC NULLS FIRST) qty_dense_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY price DESC NULLS LAST) price_cume_dist,
        STDDEV_SAMP(qty) OVER (PARTITION BY promo) promo_stddev_samp_qty
    FROM flattened
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, d.city
    FROM with_windows e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(promo_stddev_samp_qty * 100, 2) promo_stddev_samp_qty_scaled,
        COALESCE(city, 'UNKNOWN') resolved_city
    FROM with_dims
    WHERE promo IS NOT NULL
)
SELECT
    id,
    customer_id,
    coupon_codes,
    product_id,
    qty,
    price,
    promo,
    qty_dense_rank,
    price_cume_dist
FROM valid
ORDER BY product_id DESC;
