WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(created_at, 'MMM yyyy') created_at_label
    FROM flattened
    WHERE created_at > DATE '2025-03-02'
      AND value IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.created_at,
        s.created_at_label,
        p.name,
        p.segment,
        p.credit_limit
    FROM valid s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, b.signup_date
    FROM enriched s
    LEFT JOIN customers b
        ON s.customer_id = b.id
    WHERE b.segment = 'Consumer'
)
SELECT
    created_at,
    created_at_label,
    segment,
    credit_limit,
    signup_date
FROM joined;
