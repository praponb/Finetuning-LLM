WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(code) AS code_upper
    FROM flattened
    WHERE created_at > TO_TIMESTAMP('2025-03-01 15:00:00')
      AND customer_id IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        TO_DATE(created_at) AS created_at_date
    FROM cleaned
    WHERE created_at > TO_TIMESTAMP('2025-03-01 17:00:00')
      AND code IS DISTINCT FROM 'VIP'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.country,
        c.signup_date,
        c.segment
    FROM filtered s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT s.*, b.signup_date AS customer_signup_date
    FROM with_dims s
    JOIN customers b
        ON s.customer_id = b.id
    WHERE b.age <> 34
)
SELECT
    id,
    created_at,
    created_at_date,
    country,
    signup_date,
    customer_signup_date
FROM with_dims_carts
ORDER BY created_at DESC NULLS LAST, signup_date;
