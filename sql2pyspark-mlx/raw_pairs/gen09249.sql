WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
with_dims AS (
    SELECT
        a.customer_id,
        a.created_at,
        c.city,
        c.age
    FROM flattened a
    INNER JOIN customers c
        ON a.customer_id = c.id
    WHERE LENGTH(c.email) >= 13
),
base AS (
    SELECT *, CAST(age AS DOUBLE) AS age_str
    FROM with_dims
    WHERE created_at >= TIMESTAMP '2025-03-02 03:00:00'
      AND city IS NOT NULL
),
cleaned AS (
    SELECT *, DATE_TRUNC('WEEK', created_at) AS created_at_period
    FROM base
    WHERE created_at >= TIMESTAMP '2025-03-01 14:00:00'
      AND customer_id = 38
)
SELECT *
FROM (
    SELECT
        customer_id,
        city,
        age_str
    FROM cleaned
) src
PIVOT (
    AVG(age_str) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('New York' AS new_york, 'Singapore' AS singapore)
)
