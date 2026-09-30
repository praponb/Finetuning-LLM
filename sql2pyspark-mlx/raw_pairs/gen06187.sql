WITH base AS (
    SELECT *, LOWER(status) AS status_lower
    FROM subscriptions
    WHERE end_date >= DATE '2023-10-07'
      AND MONTH(start_date) IN (1, 6, 9)
),
latest AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC NULLS LAST, id) = 1
),
with_dims AS (
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.start_date,
        a.monthly_fee,
        b.age,
        b.name,
        b.city
    FROM latest a
    JOIN customers b
        ON a.customer_id = b.id
),
attributed AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_dims a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date <= DATE_ADD(sub.end_date, 3)
),
totals AS (
    SELECT
        DATE_TRUNC('MONTH', start_date) AS activity_month,
        subscription_plan,
        STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        AVG(age) AS avg_age,
        MIN(subscription_id) AS min_subscription_id,
        SIZE(COLLECT_SET(subscription_plan)) AS num_subscription_plans,
        PERCENTILE_APPROX(subscription_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS subscription_monthly_fee_quartiles,
        GROUPING(DATE_TRUNC('MONTH', start_date)) AS grouping_level
    FROM attributed
    GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', start_date), subscription_plan), (DATE_TRUNC('MONTH', start_date)))
    HAVING num_subscription_plans > 1
)
SELECT
    activity_month,
    subscription_plan,
    num_plans,
    avg_age,
    min_subscription_id,
    num_subscription_plans,
    subscription_monthly_fee_quartiles,
    grouping_level
FROM totals
ORDER BY grouping_level
