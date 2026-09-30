WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, value
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.attr_key,
        p.city,
        p.segment,
        p.age
    FROM exploded e
    LEFT JOIN customers p
        ON e.customer_id = p.id
    WHERE p.country = 'JP'
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.city,
        s.age,
        p.signup_date,
        p.country
    FROM with_dims s
    INNER JOIN customers p
        ON s.customer_id <=> p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        city,
        AVG(age) OVER (PARTITION BY country) AS country_avg_age,
        COUNT(age) OVER (PARTITION BY country) AS country_count_age,
        PERCENT_RANK() OVER (ORDER BY age DESC NULLS FIRST) AS age_percent_rank
    FROM joined
)
SELECT
    customer_id,
    country_count_age,
    age_percent_rank
FROM metrics
ORDER BY age_percent_rank NULLS LAST, country_count_age;
