SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions WHERE monthly_fee BETWEEN 100 AND 170)
PIVOT (SUM(monthly_fee) FOR status IN ('PAUSED', 'EXPIRED', 'ACTIVE'))
