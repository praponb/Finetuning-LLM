WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(start_date) AS start_date_month_end,
        ADD_MONTHS(start_date, 12) AS start_date_plus_months
    FROM subscriptions
    WHERE start_date >= DATE '2023-03-22'
      AND customer_id IS NOT NULL
),
filtered_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        ROUND(monthly_fee, 0) AS monthly_fee_rounded
    FROM filtered
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.end_date,
        a.monthly_fee,
        a.status,
        a.start_date_month_end,
        a.start_date_plus_months,
        a.end_date_month_end,
        a.monthly_fee_rounded,
        p.signup_date
    FROM filtered_subscriptions a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM enriched src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date_plus_months >= sub.start_date
       AND src.start_date_plus_months < sub.end_date + INTERVAL '7' DAY
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        monthly_fee,
        start_date_month_end,
        end_date_month_end,
        signup_date,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date_plus_months, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior,
        MAX(monthly_fee) OVER (PARTITION BY DATE_TRUNC('month', start_date_month_end), customer_id) AS start_date_month_end_period_max_monthly_fee,
        AVG(subscription_id) OVER (ORDER BY signup_date, id, subscription_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_5_prior
    FROM attributed
    WHERE subscription_monthly_fee <= 120
)
-- Join a dimension and aggregate
SELECT
    p.segment,
    p.city,
    COUNT(*) AS rows_total,
    AVG(s.subscription_monthly_fee) AS avg_subscription_monthly_fee,
    SUM(s.subscription_id) AS total_subscription_id,
    GROUPING_ID() AS gid
FROM enriched_subscriptions s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY GROUPING SETS ((p.segment, p.city), (p.segment))
ORDER BY segment DESC;
