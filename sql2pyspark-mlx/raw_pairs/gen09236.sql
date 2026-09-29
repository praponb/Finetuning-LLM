WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS tag
),
enriched AS (
    SELECT
        src.*,
        b.credit_limit,
        b.age,
        b.city
    FROM exploded src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.email IS NULL
),
joined AS (
    SELECT
        s.*,
        c.city AS customer_city,
        c.age AS customer_age,
        c.email
    FROM enriched s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
scored AS (
    SELECT
        customer_id,
        created_at,
        tag,
        age,
        city,
        email,
        RANK() OVER (ORDER BY age DESC NULLS FIRST) age_rank,
        SUM(customer_age) OVER (PARTITION BY tag ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) customer_age_sum_30m,
        ROUND(customer_age / SUM(customer_age) OVER (PARTITION BY TO_DATE(created_at), customer_city), 4) customer_age_share,
        RANK() OVER (PARTITION BY city, customer_id ORDER BY customer_age DESC NULLS LAST) customer_age_rank,
        CUME_DIST() OVER (PARTITION BY tag ORDER BY age NULLS FIRST) age_cume_dist
    FROM joined
),
with_dims AS (
    SELECT s.*, b.email AS customer_email
    FROM scored s
    JOIN customers b
        ON s.customer_id = b.id
),
remaining_keys AS (
    SELECT customer_id
    FROM with_dims
    EXCEPT
    SELECT customer_id
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
),
kept_rows AS (
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.customer_id = k.customer_id
),
grouped AS (
    SELECT
        city,
        ROUND(AVG(age), 2) AS avg_age,
        COUNT(DISTINCT tag) AS distinct_tags,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes
    FROM kept_rows
    GROUP BY city
)
SELECT city, span_minutes
FROM grouped
ORDER BY city DESC NULLS FIRST
LIMIT 5;
