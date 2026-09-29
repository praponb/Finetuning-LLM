SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions) src PIVOT (SUM(monthly_fee) FOR plan IN ('basic' AS basic, 'premium' AS premium));
