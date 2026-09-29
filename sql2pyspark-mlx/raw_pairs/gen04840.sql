WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, b.age
    FROM lines a
    LEFT JOIN customers b
        ON a.customer_id = b.id
    WHERE b.signup_date <= '2023-07-01'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN age >= 64 THEN 'High' WHEN age >= 30 THEN 'Medium' ELSE 'Low' END AS age_level
    FROM enriched
    WHERE code <> 'BUNDLE'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.code,
        c.age AS customer_age
    FROM valid a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.code,
        c.email,
        c.segment
    FROM joined s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT e.*, d.age
    FROM enriched_carts e
    LEFT JOIN customers d
        ON e.customer_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 69 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    SUM(CASE WHEN code = 'SPRING10' THEN age ELSE 0 END) AS spring10_age
FROM joined_carts
GROUP BY CASE WHEN age >= 69 THEN 'LARGE' ELSE 'SMALL' END
HAVING spring10_age > 0
ORDER BY spring10_age DESC
