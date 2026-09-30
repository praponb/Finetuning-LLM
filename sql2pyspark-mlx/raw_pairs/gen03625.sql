WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, attr_value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        attr_key,
        value
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) li AS value
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.attr_key,
        p.city
    FROM exploded a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
    WHERE p.segment IN ('Consumer', 'SMB')
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, d.name
    FROM enriched a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.attr_key,
        d.credit_limit,
        d.signup_date
    FROM with_dims e
    LEFT JOIN customers d
        ON e.customer_id = d.id
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    dim.country,
    e.attr_key,
    COUNT(*) AS num_rows,
    MAX(e.credit_limit) AS max_credit_limit
FROM enriched_carts e
LEFT JOIN customers dim ON e.customer_id = dim.id
GROUP BY dim.segment, dim.country, e.attr_key
ORDER BY country
