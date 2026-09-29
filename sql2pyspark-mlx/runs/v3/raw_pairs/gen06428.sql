SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE start_date BETWEEN '2023-12-19' AND '2025-06-08') PIVOT (COUNT(monthly_fee) FOR plan IN ('enterprise', 'premium'));
