SELECT
    end_date,
    plan,
    DATEDIFF(CURRENT_DATE(), start_date) days_since_start_date,
    SUBSTRING(plan, 1, 3) plan_prefix
FROM subscriptions
WHERE plan != 'premium'
ORDER BY plan;
