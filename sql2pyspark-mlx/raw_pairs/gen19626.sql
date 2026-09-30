WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(city, 'NONE') AS city_clean
    FROM customers
    WHERE city IS NOT NULL
      AND segment IS DISTINCT FROM 'SMB'
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.segment,
        s.is_active,
        s.age,
        s.city_title,
        x.start_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.channel IN ('mobile', 'partner', 'store')
        )
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.subscription_monthly_fee,
        top1.plan
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee AS subscription_monthly_fee, c.plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    segment,
    CASE WHEN subscription_monthly_fee >= 212 THEN 'HIGH' ELSE 'LOW' END AS subscription_monthly_fee_tier,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    BIT_AND(age) AS age_bit_and,
    MAX(age) AS max_age,
    MIN(age) AS min_age,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM latest_match
GROUP BY ROLLUP (segment, CASE WHEN subscription_monthly_fee >= 212 THEN 'HIGH' ELSE 'LOW' END);
