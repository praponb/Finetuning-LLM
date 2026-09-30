SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions) PIVOT (COUNT(monthly_fee) FOR plan IN ('enterprise', 'basic')) ORDER BY status
