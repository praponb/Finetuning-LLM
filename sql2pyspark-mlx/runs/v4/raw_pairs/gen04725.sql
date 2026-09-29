WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, p.age
    FROM exploded s
    JOIN customers p
        ON s.customer_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(age) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS age_sum_1h,
        DENSE_RANK() OVER (PARTITION BY customer_id, code ORDER BY age NULLS LAST) AS age_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY age DESC NULLS FIRST) AS age_percent_rank
    FROM joined
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    COUNT(*) AS cnt,
    MIN(created_at) AS first_created_at,
    ROUND(AVG(age), 2) AS avg_age,
    SUM(age_percent_rank) AS total_age_percent_rank
FROM enriched
GROUP BY customer_id
HAVING SUM(age_percent_rank) >= 0
ORDER BY customer_id NULLS FIRST;
