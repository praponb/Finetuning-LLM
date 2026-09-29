SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions)
PIVOT (AVG(monthly_fee) FOR status IN ('EXPIRED', 'PAUSED', 'CANCELLED', 'ACTIVE'))
ORDER BY plan
