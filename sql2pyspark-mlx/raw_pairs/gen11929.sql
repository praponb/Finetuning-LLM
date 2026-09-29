SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions) src PIVOT (AVG(monthly_fee) AS value_sum, COUNT(1) AS item_count FOR plan IN ('standard', 'enterprise', 'premium'));
