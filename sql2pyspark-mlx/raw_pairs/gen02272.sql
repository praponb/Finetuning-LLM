SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions) PIVOT (AVG(monthly_fee) FOR status IN ('ACTIVE', 'PAUSED', 'EXPIRED')) ORDER BY plan;
