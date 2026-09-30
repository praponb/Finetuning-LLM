SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions) PIVOT (SUM(monthly_fee) FOR status IN ('PAUSED', 'EXPIRED', 'CANCELLED', 'ACTIVE')) ORDER BY plan;
