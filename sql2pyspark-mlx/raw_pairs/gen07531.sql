WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        v,
        EXPLODE(coupon_codes) AS element
    FROM flattened
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.v,
        c.signup_date,
        c.country
    FROM lines src
    LEFT JOIN customers c
        ON src.customer_id = c.id
    WHERE c.age BETWEEN 59 AND 64
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.signup_date AS customer_signup_date,
        p.country AS customer_country
    FROM enriched src
    JOIN customers p
        ON src.customer_id = p.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.v,
        e.signup_date,
        e.customer_country,
        d.age,
        d.credit_limit,
        d.country AS customer_country_2
    FROM joined e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        customer_country_2,
        MAX(credit_limit) FILTER (WHERE customer_country = 'TH') AS th_max_credit_limit,
        ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN customer_country_2 = 'JP' THEN 1 END), 0), 2) AS age_per_jp,
        COUNT(*) AS cnt
    FROM enriched_carts
    GROUP BY customer_country_2
)
SELECT
    customer_country_2,
    th_max_credit_limit,
    cnt
FROM totals
ORDER BY th_max_credit_limit DESC NULLS LAST;
