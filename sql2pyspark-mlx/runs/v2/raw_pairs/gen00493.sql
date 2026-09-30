SELECT * FROM (SELECT QUARTER(end_date) AS end_quarter, plan, monthly_fee FROM subscriptions WHERE status = 'EXPIRED')
PIVOT (COUNT(monthly_fee) FOR plan IN ('enterprise', 'premium', 'standard'));
