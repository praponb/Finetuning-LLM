WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        p.city,
        p.segment
    FROM exploded e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.segment,
        b.email,
        b.country,
        b.city AS customer_city
    FROM enriched s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
    WHERE b.name IS NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.age,
        c.credit_limit
    FROM enriched_carts src
    INNER JOIN customers c
        ON src.customer_id <=> c.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.email AS customer_email,
        c.age AS customer_age,
        c.country AS customer_country
    FROM joined a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        segment,
        email,
        country,
        age,
        customer_age,
        customer_country,
        UPPER(country) AS country_upper
    FROM joined_carts
    WHERE customer_country IS NOT NULL
      AND credit_limit < 14800
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        segment,
        email,
        country,
        age,
        CASE WHEN customer_age >= 68 THEN 'High' WHEN customer_age >= 32 THEN 'Medium' ELSE 'Low' END AS customer_age_level,
        COALESCE(customer_country, 'NONE') AS resolved_customer_country
    FROM filtered
    WHERE segment IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    email,
    country,
    resolved_customer_country,
    CUME_DIST() OVER (ORDER BY age NULLS FIRST) AS age_cume_dist
FROM base
