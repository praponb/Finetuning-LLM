SELECT id, status, CONCAT_WS('-', status, plan) status_plan_key FROM subscriptions WHERE YEAR(end_date) = 2024
