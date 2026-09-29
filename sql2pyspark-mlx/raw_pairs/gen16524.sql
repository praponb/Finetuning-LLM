SELECT monthly_fee, plan, DATEDIFF(CURRENT_DATE(), start_date) / 365.25 AS years_since_start_date
FROM subscriptions
ORDER BY plan
