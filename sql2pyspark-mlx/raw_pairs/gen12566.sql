WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS code
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.code,
        c.segment
    FROM lines a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, d.country
    FROM with_dims s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(country) AS country_upper,
        CONCAT(country, ' - ', code) AS country_code
    FROM joined
    WHERE customer_id IS NOT NULL
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.segment AS customer_segment,
        b.email
    FROM filtered src
    LEFT JOIN customers b
        ON src.customer_id = b.id
)
-- Multi-level aggregation
SELECT
    country,
    customer_segment,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT_IF(country = 'JP') AS jp_events,
    COUNT(*) AS event_count
FROM joined_carts
GROUP BY ROLLUP (country, customer_segment)
HAVING COUNT_IF(country = 'SG') < 1000
ORDER BY country NULLS FIRST;
