SELECT
    id,
    monthly_fee,
    end_date,
    CASE WHEN plan IN ('basic', 'enterprise') THEN 'Group A' ELSE 'Other' END AS plan_group
FROM subscriptions
WHERE plan IN ('premium', 'basic') OR start_date BETWEEN '2024-09-01' AND '2025-01-04'
