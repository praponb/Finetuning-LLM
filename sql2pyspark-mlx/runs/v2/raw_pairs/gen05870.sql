SELECT id, customer_id, monthly_fee, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS row_num FROM subscriptions
