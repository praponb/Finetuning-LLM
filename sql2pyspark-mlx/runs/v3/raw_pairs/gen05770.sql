SELECT plan, ROUND(AVG(monthly_fee), 2) avg_monthly_fee, COUNT(*) num_subscriptions FROM subscriptions GROUP BY plan
