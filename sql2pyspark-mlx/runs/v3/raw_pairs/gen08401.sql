SELECT QUARTER(end_date) end_quarter, MAX(monthly_fee) max_monthly_fee FROM subscriptions GROUP BY 1 HAVING max_monthly_fee < 120 ORDER BY max_monthly_fee DESC, end_quarter LIMIT 10
