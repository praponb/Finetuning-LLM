SELECT
    id,
    end_date,
    status,
    start_date,
    CASE WHEN plan IN ('enterprise', 'basic') THEN 'Core' ELSE 'Other' END AS plan_group,
    QUARTER(start_date) AS start_date_quarter
FROM subscriptions
WHERE end_date < '2025-01-01'
ORDER BY plan_group DESC, id
LIMIT 50;
