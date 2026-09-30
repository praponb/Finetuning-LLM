SELECT customer_id, plan, MIN(start_date) AS first_start_date, COUNT(*) AS subscription_count FROM subscriptions GROUP BY ROLLUP(customer_id, plan) ORDER BY customer_id;
