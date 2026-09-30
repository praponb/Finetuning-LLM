SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions) PIVOT (COUNT(monthly_fee) FOR status IN ('PAUSED', 'ACTIVE')) ORDER BY plan
