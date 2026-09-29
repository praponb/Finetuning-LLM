WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS value
),
enriched AS (
    SELECT
        a.*,
        p.name,
        p.age
    FROM exploded a
    INNER JOIN customers p
        ON a.customer_id = p.id
    WHERE p.is_active
),
with_dims AS (
    SELECT src.*, b.name AS customer_name
    FROM enriched src
    INNER JOIN customers b
        ON src.customer_id <=> b.id
),
base AS (
    SELECT *, LENGTH(name) AS name_length
    FROM with_dims
    WHERE value != 'VIP'
)
SELECT
    id,
    customer_id,
    created_at,
    name,
    age,
    CUME_DIST() OVER (ORDER BY age DESC NULLS LAST) AS age_cume_dist
FROM base;
