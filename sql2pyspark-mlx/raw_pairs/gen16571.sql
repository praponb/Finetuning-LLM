SELECT DATE_TRUNC('MONTH', end_date) AS end_month, plan, MAX(start_date) AS last_start_date FROM subscriptions WHERE customer_id = 33 GROUP BY DATE_TRUNC('MONTH', end_date), plan;
