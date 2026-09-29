WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE plan IN ('premium', 'standard')
),
cleaned_subscriptions AS (
    SELECT *, DATE_ADD(start_date, 72) AS start_date_plus_72d
    FROM cleaned
    WHERE customer_id IS NOT NULL
      AND start_date >= '2023-04-07'
),
with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM cleaned_subscriptions src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date_plus_72d >= sub.start_date
       AND src.start_date_plus_72d < sub.end_date + INTERVAL 7 DAYS
),
with_windows AS (
    SELECT
        *,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY subscription_plan), 4) AS subscription_id_share,
        SUM(subscription_id) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_subscription_id,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER w_recent AS first_known_monthly_fee
    FROM with_window
    WHERE end_date >= '2025-02-01'
    WINDOW w_recent AS (PARTITION BY customer_id, subscription_plan ORDER BY start_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.end_date,
        src.monthly_fee,
        src.status,
        src.days_since_end_date,
        src.start_date_plus_72d,
        src.subscription_id,
        src.subscription_plan,
        src.subscription_monthly_fee,
        src.subscription_id_share,
        src.cumulative_subscription_id,
        src.first_known_monthly_fee,
        p.signup_date,
        p.email
    FROM with_windows src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
SELECT
    s.id,
    s.customer_id,
    s.monthly_fee,
    s.status,
    s.days_since_end_date,
    s.start_date_plus_72d,
    s.subscription_id,
    s.subscription_plan,
    s.cumulative_subscription_id,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM enriched s
ORDER BY subscription_plan NULLS FIRST;
