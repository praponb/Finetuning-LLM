SELECT
    id,
    start_date,
    status,
    end_date,
    CONCAT_WS('-', status, plan) AS status_plan_key
FROM subscriptions
WHERE plan = 'basic'
