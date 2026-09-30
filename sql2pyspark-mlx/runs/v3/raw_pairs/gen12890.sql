SELECT
    customer_id,
    plan,
    end_date,
    DATEDIFF(CURRENT_DATE(), end_date) days_since_end_date,
    SUBSTRING(plan, 1, 4) plan_prefix
FROM subscriptions
WHERE NOT (plan = 'basic' OR plan = 'standard')
ORDER BY end_date;
