WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(status, ' - ', plan) AS status_plan,
        CONCAT_WS(',', plan, status) AS plan_status_key
    FROM subscriptions
    WHERE start_date >= DATE '2023-08-15'
),
cleaned_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(plan) AS clean_plan
    FROM cleaned
    WHERE end_date >= DATE '2023-04-12'
      AND monthly_fee BETWEEN 210 AND 240
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee NULLS LAST) AS monthly_fee_rank,
        DENSE_RANK() OVER (PARTITION BY customer_id, status ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_dense_rank,
        LEAD(monthly_fee, 1) OVER (PARTITION BY status ORDER BY start_date, id) AS next_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY plan, status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM cleaned_subscriptions
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM with_windows
    INTERSECT ALL
    SELECT status
    FROM subscriptions
    WHERE plan = 'enterprise'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.status = k.status
)
SELECT
    s.customer_id,
    s.plan,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.status_plan,
    s.plan_status_key,
    s.clean_plan,
    s.monthly_fee_rank,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM verified s
