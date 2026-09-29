WITH with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.subscription_status,
        top1.subscription_end_date
    FROM customers s
    JOIN LATERAL (
            SELECT c.status AS subscription_status, c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.subscription_end_date
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND TO_DATE(ch.started_at) >= '2025-02-01'
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        email,
        age
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_status ORDER BY subscription_end_date DESC NULLS FIRST, id) = 1
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.discount < 0.3
        )
),
not_flagged_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.country IN ('JP', 'TH')
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(age) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_age,
        COUNT(age) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_age,
        AVG(age) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_age,
        SUM(age) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_age,
        LAG(age, 1) OVER (ORDER BY age, id) AS prev_age
    FROM not_flagged_customers
    WHERE email LIKE '%@company.co'
)
-- Grouped metrics
SELECT
    CASE WHEN last3_avg_age >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_avg_age_tier,
    APPROX_PERCENTILE(prev_age, ARRAY(0.25, 0.5, 0.75)) AS prev_age_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(age) AS age_bit_and,
    ROUND(AVG(last3_max_age), 2) AS avg_last3_max_age
FROM enriched
GROUP BY CASE WHEN last3_avg_age >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_last3_max_age DESC NULLS LAST;
