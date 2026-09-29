WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS line
),
flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        product_id,
        price,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
with_windows AS (
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY MONTH(created_at)), 4) AS price_share,
        PERCENT_RANK() OVER (PARTITION BY customer_id, code ORDER BY product_id DESC NULLS FIRST) AS product_id_percent_rank,
        PERCENT_RANK() OVER w_recent AS price_percent_rank,
        RANK() OVER w_hist AS product_id_rank,
        CUME_DIST() OVER (PARTITION BY code ORDER BY product_id DESC NULLS FIRST) AS product_id_cume_dist
    FROM flattened
    WHERE HOUR(created_at) BETWEEN 10 AND 18
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY price ASC), w_hist AS (PARTITION BY code ORDER BY product_id DESC)
),
with_dims AS (
    SELECT
        a.customer_id,
        a.product_id,
        a.price,
        a.code,
        a.product_id_rank,
        c.credit_limit,
        c.segment
    FROM with_windows a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
with_dims_carts AS (
    SELECT
        s.*,
        b.city,
        b.country,
        b.age
    FROM with_dims s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    country,
    metric_name,
    value
FROM (
    SELECT
        country,
        price,
        credit_limit
    FROM with_dims_carts
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (price AS PRICE, credit_limit AS CREDIT_LIMIT_VAL)
) long_rows
