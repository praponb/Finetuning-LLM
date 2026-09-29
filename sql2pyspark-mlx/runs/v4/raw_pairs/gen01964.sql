SELECT
    id,
    plan,
    customer_id,
    DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
    CASE WHEN plan IN ('basic', 'standard') THEN 'Tier 1' ELSE 'Other' END AS plan_group
FROM subscriptions
WHERE monthly_fee BETWEEN 110 AND 220 AND customer_id IS NOT NULL
ORDER BY plan_group, id
LIMIT 10
