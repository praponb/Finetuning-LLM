SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions) PIVOT (SUM(monthly_fee) FOR status IN ('CANCELLED', 'EXPIRED')) ORDER BY plan
