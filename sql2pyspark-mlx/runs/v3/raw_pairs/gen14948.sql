SELECT ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, MIN(start_date) AS min_start_date FROM subscriptions WHERE plan = 'standard'
