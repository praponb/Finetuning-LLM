WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        value,
        EXPLODE_OUTER(coupon_codes) AS value_2
    FROM lines
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM exploded
    INTERSECT
    SELECT customer_id, id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.created_at,
        a.value,
        a.value_2,
        p.city,
        p.country
    FROM reconciled a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE p.email IS NOT NULL
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        customer_id,
        MIN(created_at) AS started_at,
        SIZE(COLLECT_SET(value_2)) AS num_value_2s,
        COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM joined
    GROUP BY 1
)
SELECT
    customer_id,
    started_at,
    num_value_2s,
    singapore_count
FROM grouped
ORDER BY customer_id DESC, singapore_count
LIMIT 5
