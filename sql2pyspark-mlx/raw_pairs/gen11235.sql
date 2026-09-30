WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, attr_value
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        attr_key,
        attr_value,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.attr_key,
        c.segment,
        c.email,
        c.signup_date
    FROM exploded_carts src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.age,
        p.email AS customer_email
    FROM with_dims s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
    WHERE p.age >= 64
)
SELECT
    customer_id,
    attr_key,
    age,
    customer_email
FROM with_dims_carts
ORDER BY attr_key DESC;
