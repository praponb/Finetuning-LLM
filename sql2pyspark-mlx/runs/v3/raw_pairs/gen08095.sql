SELECT *, TRIM(plan) AS clean_plan, INITCAP(TRIM(plan)) AS plan_title
FROM subscriptions
WHERE customer_id = 2 AND (status IN ('PAUSED', 'CANCELLED') OR start_date < '2023-06-01')
ORDER BY clean_plan, id
LIMIT 5;
