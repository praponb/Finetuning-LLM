WITH valid_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE plan IN ('standard', 'premium') AND (customer_id IS NULL OR end_date > '2023-04-07')
),
plan_summary AS (
    SELECT plan, MIN(monthly_fee) AS min_monthly_fee, MAX(monthly_fee) AS max_monthly_fee
    FROM valid_subscriptions
    GROUP BY plan
)
SELECT plan, min_monthly_fee, max_monthly_fee
FROM plan_summary
WHERE max_monthly_fee < 70
ORDER BY 2 DESC;
