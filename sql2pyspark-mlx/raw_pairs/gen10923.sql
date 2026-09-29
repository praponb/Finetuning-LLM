SELECT customer_id, COUNT(monthly_fee) AS monthly_fee_count FROM subscriptions GROUP BY customer_id HAVING COUNT(monthly_fee) >= 1
