WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        p.signup_date,
        p.segment,
        p.name
    FROM flattened e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
    WHERE p.signup_date >= '2024-08-29'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.age,
        b.email,
        b.signup_date AS customer_signup_date
    FROM with_dims a
    INNER JOIN customers b
        ON a.customer_id = b.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(age) OVER (PARTITION BY customer_id) AS customer_id_max_age,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share,
        COUNT(age) OVER (PARTITION BY customer_id) AS customer_id_count_age,
        ROUND(age / SUM(age) OVER (PARTITION BY customer_id), 4) AS age_share_2
    FROM joined
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        signup_date,
        age,
        email,
        customer_signup_date,
        customer_id_max_age,
        age_share_2,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY age_share) AS age_share_cume_dist,
        ROUND(customer_id_max_age / SUM(customer_id_max_age) OVER (PARTITION BY customer_id), 4) AS customer_id_max_age_share,
        MIN(age_share_2) OVER (PARTITION BY customer_id) AS customer_id_min_age_share_2
    FROM with_windows
)
SELECT
    signup_date,
    age,
    customer_signup_date,
    age_share_2,
    age_share_cume_dist,
    customer_id_max_age_share,
    customer_id_min_age_share_2
FROM enriched
ORDER BY signup_date NULLS LAST
