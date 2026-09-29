SELECT customer_id, COUNT(DISTINCT monthly_fee) AS unique_statuss FROM subscriptions GROUP BY customer_id
