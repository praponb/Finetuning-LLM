SELECT customer_id, plan, status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range FROM subscriptions GROUP BY customer_id, plan, status HAVING AVG(monthly_fee) >= 170;
