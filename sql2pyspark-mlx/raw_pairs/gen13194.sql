WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        age,
        LENGTH(country) AS country_length
    FROM customers
    WHERE signup_date >= TO_DATE('2023-05-22')
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS row_num
    FROM base
    QUALIFY row_num = 1
),
ranked_customers AS (
    SELECT
        id,
        name,
        email,
        age
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY country_length DESC, id) AS latest_rank
        FROM ranked
    ) ranked
    WHERE latest_rank = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.country NOT IN ('SG', 'TH')
        )
),
with_windows AS (
    SELECT
        id,
        name,
        LEAD(age, 1) OVER (ORDER BY age, id) AS next_age,
        ROW_NUMBER() OVER (ORDER BY age DESC, id) AS event_seq
    FROM not_flagged
),
with_latest AS (
    SELECT
        s.*,
        top1.subscription_plan,
        top1.subscription_monthly_fee
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan, c.monthly_fee AS subscription_monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN subscription_monthly_fee >= 193 THEN 'HIGH' ELSE 'LOW' END AS subscription_monthly_fee_tier,
    AVG(subscription_monthly_fee) AS avg_subscription_monthly_fee,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(subscription_monthly_fee) FILTER (WHERE subscription_plan = 'enterprise') AS enterprise_max_subscription_monthly_fee,
    COUNT_IF(subscription_plan = 'premium') AS premium_events,
    SUM(CASE WHEN subscription_plan = 'enterprise' THEN next_age ELSE 0 END) AS enterprise_next_age,
    COUNT(*) FILTER (WHERE subscription_plan = 'enterprise') AS enterprise_rows
FROM with_latest
GROUP BY CASE WHEN subscription_monthly_fee >= 193 THEN 'HIGH' ELSE 'LOW' END;
