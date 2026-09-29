WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        d.name,
        d.email
    FROM flattened src
    JOIN customers d
        ON src.customer_id = d.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(name) clean_name
    FROM enriched
    WHERE name NOT LIKE 'Ja%'
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.name,
        e.clean_name,
        b.email AS customer_email,
        b.credit_limit
    FROM base e
    LEFT JOIN customers b
        ON e.customer_id = b.id
    WHERE b.city IS NOT NULL
),
enriched_carts_2 AS (
    -- Attach customers attributes
    SELECT a.*, d.name AS customer_name
    FROM enriched_carts a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.name,
        s.customer_email,
        s.credit_limit,
        d.name AS customer_name_2
    FROM enriched_carts_2 s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
)
SELECT
    customer_id,
    customer_email,
    credit_limit
FROM with_dims
