WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS code
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN code = 'WELCOME5' THEN 1 ELSE 0 END is_welcome5
    FROM flattened
    WHERE customer_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY is_welcome5 DESC NULLS FIRST) is_welcome5_dense_rank,
        RANK() OVER (ORDER BY is_welcome5 NULLS FIRST) is_welcome5_rank,
        MAX(is_welcome5) OVER (PARTITION BY customer_id) customer_id_max_is_welcome5,
        AVG(is_welcome5) OVER (PARTITION BY code) code_avg_is_welcome5,
        SUM(is_welcome5) OVER (PARTITION BY customer_id) customer_id_sum_is_welcome5
    FROM filtered
),
metrics_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(is_welcome5) OVER (PARTITION BY code) code_sum_is_welcome5,
        ROUND(is_welcome5_rank / SUM(is_welcome5_rank) OVER (PARTITION BY customer_id), 4) is_welcome5_rank_share,
        DENSE_RANK() OVER (ORDER BY is_welcome5 DESC) is_welcome5_dense_rank_2
    FROM metrics
),
daily AS (
    -- Grouped metrics
    SELECT
        code,
        ROUND(SUM(customer_id_sum_is_welcome5) / NULLIF(COUNT(CASE WHEN code = 'SPRING10' THEN 1 END), 0), 2) AS customer_id_sum_is_welcome5_per_spring10,
        PERCENTILE_APPROX(code_sum_is_welcome5, 0.5) AS code_sum_is_welcome5_p50,
        COUNT(*) AS num_rows
    FROM metrics_carts
    GROUP BY CUBE (code)
)
SELECT
    code,
    customer_id_sum_is_welcome5_per_spring10,
    code_sum_is_welcome5_p50,
    CASE WHEN code RLIKE '^(VIP|WELCOME5)$' THEN 'CORE' WHEN code RLIKE '^(SPRING10|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS code_zone
FROM daily
ORDER BY code_sum_is_welcome5_p50 DESC, customer_id_sum_is_welcome5_per_spring10
