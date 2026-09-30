SELECT customer_id, SUM(monthly_fee) AS total_monthly_fee
FROM subscriptions
GROUP BY customer_id
HAVING SUM(monthly_fee) > 960;
