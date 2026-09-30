SELECT *, CONCAT(plan, '/', status) AS plan_status, CONCAT(status, '/', plan) AS status_plan
FROM subscriptions
WHERE customer_id IS NULL AND plan <> 'enterprise'
ORDER BY customer_id DESC;
