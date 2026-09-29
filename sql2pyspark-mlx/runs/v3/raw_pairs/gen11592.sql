SELECT status, SUM(monthly_fee) AS total_monthly_fee, RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS rnk FROM subscriptions GROUP BY status
