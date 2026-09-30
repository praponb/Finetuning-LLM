WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS line
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM exploded
    EXCEPT
    SELECT customer_id
    FROM carts
    WHERE SIZE(coupon_codes) > 1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.promo,
        p.city,
        p.signup_date
    FROM kept_rows a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.signup_date,
        p.country,
        p.email,
        p.age
    FROM joined src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.signup_date < '2024-05-21'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 70 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    ROUND(AVG(age), 2) AS avg_age,
    SIZE(COLLECT_SET(country)) AS num_countries,
    APPROX_PERCENTILE(age, 0.5) AS age_p50,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list_2
FROM enriched
GROUP BY CASE WHEN age >= 70 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_age DESC NULLS LAST;
