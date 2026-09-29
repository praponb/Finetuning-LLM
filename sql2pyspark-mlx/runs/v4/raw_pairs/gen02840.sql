SELECT * FROM (SELECT customer_id, status, plan, monthly_fee FROM subscriptions) src PIVOT (MAX(monthly_fee) AS value_sum FOR plan IN ('basic' AS basic, 'premium' AS premium));
