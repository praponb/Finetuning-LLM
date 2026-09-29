WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        segment,
        is_active,
        age,
        COALESCE(city, 'N/A') AS city_filled
    FROM customers
    WHERE credit_limit > 6500
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        segment,
        is_active,
        age,
        city_filled,
        RANK() OVER w_recent AS age_rank,
        NTILE(3) OVER (PARTITION BY segment ORDER BY age, id) AS age_bucket,
        AVG(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_5
    FROM base
    WINDOW w_recent AS (ORDER BY age NULLS LAST)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY segment ORDER BY age_bucket DESC, id) AS age_bucket_bucket,
        CUME_DIST() OVER (PARTITION BY segment ORDER BY age_bucket DESC NULLS LAST) AS age_bucket_cume_dist,
        AVG(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS age_avg_5_prior
    FROM metrics
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY age_rank DESC NULLS LAST) AS rn
    FROM scored
    QUALIFY rn = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.shipping_fee > 40
        )
)
SELECT
    id,
    segment,
    is_active,
    age,
    city_filled,
    age_rank,
    age_bucket,
    age_avg_5,
    age_bucket_bucket
FROM eligible
