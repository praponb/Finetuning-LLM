SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions)
PIVOT (MAX(monthly_fee) FOR status IN ('ACTIVE', 'CANCELLED', 'EXPIRED'));
