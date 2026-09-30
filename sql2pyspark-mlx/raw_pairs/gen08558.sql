SELECT id, customer_id, CONCAT_WS(',', plan, status) AS plan_status_key FROM subscriptions ORDER BY id DESC LIMIT 5;
