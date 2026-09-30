SELECT status, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions GROUP BY status HAVING SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) > 54
