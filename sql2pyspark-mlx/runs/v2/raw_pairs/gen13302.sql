SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions WHERE plan = 'standard') PIVOT (COUNT(monthly_fee) FOR status IN ('CANCELLED', 'PAUSED', 'ACTIVE', 'EXPIRED')) ORDER BY plan;
