SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE monthly_fee BETWEEN 190 AND 220) PIVOT (AVG(monthly_fee) FOR plan IN ('enterprise', 'premium', 'basic'))
