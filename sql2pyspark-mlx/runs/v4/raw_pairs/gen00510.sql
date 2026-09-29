WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM subscriptions s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.end_date BETWEEN sub.start_date AND sub.end_date
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date NULLS LAST, id, subscription_id) = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.name,
        d.signup_date
    FROM ranked a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        COUNT(*) AS row_count,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        SIZE(COLLECT_SET(plan)) AS num_plans
    FROM with_dims
    GROUP BY DATE_TRUNC('QUARTER', signup_date)
)
SELECT
    quarter_start,
    row_count,
    num_plans
FROM totals
ORDER BY row_count DESC;
