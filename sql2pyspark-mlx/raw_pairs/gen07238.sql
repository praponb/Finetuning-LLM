WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS key, v
),
enriched AS (
    SELECT src.*, p.signup_date
    FROM flattened src
    JOIN customers p
        ON src.customer_id = p.id
),
joined AS (
    SELECT src.*, d.city
    FROM enriched src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
    WHERE d.name LIKE 'A%'
),
enriched_carts AS (
    SELECT
        s.*,
        d.country,
        d.age
    FROM joined s
    JOIN customers d
        ON s.customer_id = d.id
)
SELECT
    id,
    coupon_codes,
    signup_date,
    city,
    country,
    ROUND(age / SUM(age) OVER (PARTITION BY country), 4) age_share,
    PERCENT_RANK() OVER (ORDER BY age NULLS FIRST) age_percent_rank,
    ROUND(age / SUM(age) OVER (PARTITION BY customer_id, key), 4) age_share_2
FROM enriched_carts;
