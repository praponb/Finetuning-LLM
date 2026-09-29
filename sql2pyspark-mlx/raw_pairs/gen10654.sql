WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', segment, email) AS segment_email_key,
        COALESCE(segment, 'N/A') AS segment_filled
    FROM customers
    WHERE city IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(age) OVER (PARTITION BY country, city ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_age,
        COUNT(age) OVER (PARTITION BY country, city ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_age,
        AVG(age) OVER (PARTITION BY country, city ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_age,
        SUM(age) OVER (PARTITION BY country, city ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_age,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_dense_rank
    FROM filtered
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date >= s.signup_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        segment,
        is_active,
        age,
        segment_email_key,
        last4_min_age,
        last4_avg_age,
        last4_sum_age,
        AVG(age) OVER (ORDER BY credit_limit_dense_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS age_avg_3,
        LAG(last4_avg_age, 2) OVER (PARTITION BY country, city ORDER BY credit_limit, id) AS prev_last4_avg_age,
        LEAD(last4_min_age, 1) OVER (PARTITION BY segment ORDER BY last4_avg_age, id) AS next_last4_min_age
    FROM eligible
    WHERE city IN ('Singapore', 'Chiang Mai', 'Bangkok')
),
summary AS (
    -- Multi-level aggregation
    SELECT
        city,
        segment,
        CASE WHEN last4_min_age >= 58 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_age_tier,
        COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
        COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
        AVG(age) FILTER (WHERE city = 'New York') AS new_york_avg_age,
        AVG(age_avg_3) FILTER (WHERE city = 'New York') AS new_york_avg_age_avg_3,
        ROUND(AVG(age_avg_3), 2) AS avg_age_avg_3,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY AVG(age) FILTER (WHERE city = 'New York') DESC) AS new_york_avg_age_percent_rank
    FROM scored
    GROUP BY 1, 2, 3
    HAVING new_york_avg_age_avg_3 > 0
)
SELECT
    city,
    segment,
    last4_min_age_tier,
    corporate_rows,
    new_york_rows,
    new_york_avg_age,
    avg_age_avg_3,
    distinct_ids
FROM summary
ORDER BY avg_age_avg_3
