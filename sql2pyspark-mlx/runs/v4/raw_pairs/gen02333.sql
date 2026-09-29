WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS tag
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.city,
        b.email
    FROM flattened s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.signup_date,
        p.email AS customer_email,
        p.name
    FROM with_dims a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, c.country
    FROM joined a
    LEFT JOIN customers c
        ON a.customer_id = c.id
)
-- Multi-level aggregation
SELECT
    tag,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MIN(created_at) AS first_seen,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM enriched
GROUP BY ROLLUP (tag, DATE_TRUNC('DAY', created_at))
HAVING cnt < 1
ORDER BY activity_day DESC;
