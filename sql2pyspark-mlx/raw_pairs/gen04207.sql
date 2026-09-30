WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
joined AS (
    SELECT
        s.*,
        p.country,
        p.name,
        p.email
    FROM flattened s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
base AS (
    SELECT *, COALESCE(element, 'Unknown') AS element_filled
    FROM joined
    WHERE element IS NOT NULL
),
filtered AS (
    SELECT
        customer_id,
        created_at,
        element,
        country,
        name,
        email,
        element_filled,
        TRIM(name) AS clean_name
    FROM base
    WHERE country IS NOT NULL
      AND created_at >= TIMESTAMP '2025-03-01 18:00:00'
)
SELECT
    customer_id,
    element,
    name,
    email,
    element_filled
FROM filtered
ORDER BY element
