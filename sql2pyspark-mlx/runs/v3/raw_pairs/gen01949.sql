SELECT
    id,
    customer_id,
    LAST_DAY(end_date) AS end_date_month_end,
    CASE WHEN plan IN ('standard', 'enterprise') THEN 'Priority' ELSE 'Other' END AS plan_group
FROM subscriptions
WHERE (YEAR(end_date) = 2024 AND customer_id IS NULL) OR customer_id IS NULL
ORDER BY plan_group DESC
