WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS line
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.signup_date,
        b.email,
        b.age
    FROM flattened a
    JOIN customers b
        ON a.customer_id = b.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        price,
        promo,
        signup_date,
        email,
        age,
        INITCAP(TRIM(promo)) AS promo_title,
        DATE_TRUNC('year', created_at) AS created_at_period
    FROM with_dims
    WHERE customer_id IS NOT NULL
      AND customer_id = 32
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, CUME_DIST() OVER (PARTITION BY customer_id ORDER BY age) AS age_cume_dist
    FROM cleaned
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.segment,
        b.name,
        b.city
    FROM enriched src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
with_dims_carts_2 AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.email,
        a.name,
        c.country,
        c.name AS customer_name
    FROM with_dims_carts a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.country AS customer_country,
        p.email AS customer_email
    FROM with_dims_carts_2 s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
SELECT
    customer_id,
    email,
    name,
    customer_name,
    customer_country,
    customer_email,
    CASE WHEN customer_name RLIKE '^(bob smith|David Lee )$' THEN 'CORE' ELSE 'EXTENDED' END AS customer_name_zone
FROM joined
ORDER BY customer_id NULLS LAST;
