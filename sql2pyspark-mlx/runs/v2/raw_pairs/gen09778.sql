SELECT customer_id, SUM(monthly_fee) total_monthly_fee, COUNT(*) cnt, RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk FROM subscriptions WHERE monthly_fee > 220 GROUP BY customer_id
