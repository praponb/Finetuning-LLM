SELECT customer_id, SUM(monthly_fee) AS total_monthly_fee, COUNT(*) AS cnt, SUM(SUM(monthly_fee)) OVER (ORDER BY customer_id) AS cumulative_monthly_fee FROM subscriptions GROUP BY customer_id
