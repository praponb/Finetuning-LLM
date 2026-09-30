SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE start_date <= '2025-07-01') PIVOT (MAX(monthly_fee) FOR plan IN ('standard', 'enterprise', 'basic')) ORDER BY status;
