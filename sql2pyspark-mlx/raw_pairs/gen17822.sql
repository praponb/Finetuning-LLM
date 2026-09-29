WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        WEEKOFYEAR(start_date) start_date_weekofyear,
        CASE WHEN plan = 'premium' THEN 1 ELSE 0 END is_premium
    FROM subscriptions
    WHERE start_date > DATE '2023-07-18'
),
cleaned_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(plan) plan_lower,
        INITCAP(TRIM(plan)) plan_title
    FROM cleaned
    WHERE customer_id IS NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_premium) OVER w_recent prev_is_premium,
        LAG(monthly_fee) OVER w_recent prev_monthly_fee,
        LEAD(monthly_fee) OVER w_recent next_monthly_fee,
        ROW_NUMBER() OVER w_recent event_seq,
        LAG(is_premium, 1) OVER w_recent_2 prev_is_premium_2
    FROM cleaned_subscriptions
    WHERE plan IN ('standard', 'premium', 'enterprise')
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY is_premium, id), w_recent_2 AS (PARTITION BY customer_id ORDER BY start_date_weekofyear, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, next_monthly_fee + prev_is_premium_2 next_monthly_fee_plus_prev_is_premium_2
    FROM scored
    WHERE end_date > DATE '2023-09-20'
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.monthly_fee,
    s.start_date_weekofyear,
    s.is_premium,
    s.prev_is_premium,
    s.event_seq,
    CASE WHEN plan RLIKE '^(basic|enterprise)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS plan_zone,
    (SELECT MAX(x.shipping_fee) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_shipping_fee
FROM base s
ORDER BY start_date_weekofyear NULLS LAST;
