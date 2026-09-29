WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
enriched AS (
    SELECT
        s.customer_id,
        s.value,
        d.age
    FROM lines s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
enriched_carts AS (
    SELECT
        s.*,
        d.name,
        d.age AS customer_age,
        d.segment
    FROM enriched s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
),
metrics AS (
    SELECT
        customer_id,
        value,
        name,
        customer_age,
        segment,
        ROUND(customer_age / SUM(customer_age) OVER (PARTITION BY value), 4) AS customer_age_share
    FROM enriched_carts
    WHERE value IS NOT NULL
)
SELECT
    CASE WHEN customer_age_share >= 0.7 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_age_share_tier,
    segment,
    CASE WHEN customer_age >= 70 THEN 'HIGH' ELSE 'LOW' END AS customer_age_tier,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    APPROX_PERCENTILE(customer_age_share, 0.5) AS customer_age_share_p50,
    COUNT_IF(value = 'SPRING10') AS spring10_events,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN customer_age_share >= 0.7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY APPROX_PERCENTILE(customer_age_share, 0.5) DESC) AS customer_age_share_p50_dense_rank
FROM metrics
GROUP BY CASE WHEN customer_age_share >= 0.7 THEN 'PREMIUM' ELSE 'STANDARD' END, segment, CASE WHEN customer_age >= 70 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT_IF(value = 'SPRING10') > 1
ORDER BY segment DESC;
