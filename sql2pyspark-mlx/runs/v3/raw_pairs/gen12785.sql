SELECT id, end_date, plan, CONCAT_WS('-', status, plan) AS status_plan_key FROM subscriptions
