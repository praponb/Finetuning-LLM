SELECT *, CONCAT_WS(',', status, plan) AS status_plan_key
FROM subscriptions
WHERE start_date >= '2024-06-10'
ORDER BY customer_id DESC, status_plan_key, id
LIMIT 5
