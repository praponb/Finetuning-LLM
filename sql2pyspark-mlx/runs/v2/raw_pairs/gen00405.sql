SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions) PIVOT (AVG(monthly_fee) FOR plan IN ('premium', 'basic', 'standard'));
