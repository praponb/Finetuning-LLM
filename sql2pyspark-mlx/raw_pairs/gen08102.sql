SELECT id, customer_id, monthly_fee, AVG(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_avg_monthly_fee FROM subscriptions;
