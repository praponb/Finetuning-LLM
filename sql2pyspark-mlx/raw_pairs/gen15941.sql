SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions) src PIVOT (SUM(monthly_fee) AS total, COUNT(1) AS item_count FOR plan IN ('standard', 'enterprise'));
