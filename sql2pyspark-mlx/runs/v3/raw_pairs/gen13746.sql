SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions)
PIVOT (SUM(monthly_fee) FOR plan IN ('enterprise', 'standard', 'basic'));
