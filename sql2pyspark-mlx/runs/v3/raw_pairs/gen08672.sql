SELECT id, customer_id, CONCAT_WS('-', plan, status) AS plan_status_key
FROM subscriptions
WHERE start_date < '2024-02-01'
