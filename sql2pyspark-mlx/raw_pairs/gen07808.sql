SELECT * FROM (SELECT customer_id, plan, monthly_fee FROM subscriptions) PIVOT (MAX(monthly_fee) FOR plan IN ('premium', 'enterprise', 'basic')) ORDER BY customer_id;
