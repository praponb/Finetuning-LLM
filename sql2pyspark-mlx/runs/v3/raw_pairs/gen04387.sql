SELECT * FROM (SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, plan, monthly_fee FROM subscriptions WHERE status NOT IN ('PAUSED', 'CANCELLED'))
PIVOT (AVG(monthly_fee) FOR plan IN ('basic', 'enterprise'))
ORDER BY start_month;
