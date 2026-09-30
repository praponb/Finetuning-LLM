WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        c.coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) e AS item
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        product_id,
        qty,
        price,
        promo,
        code
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.product_id,
        src.price,
        c.credit_limit,
        c.segment
    FROM exploded src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.email,
        d.country
    FROM with_dims e
    LEFT JOIN customers d
        ON e.customer_id = d.id
    WHERE d.city = 'New York'
)
SELECT
    product_id,
    credit_limit,
    segment,
    email,
    country
FROM joined;
