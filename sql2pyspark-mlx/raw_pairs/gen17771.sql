SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions) src PIVOT (SUM(monthly_fee) AS total, COUNT(1) AS n FOR status IN ('ACTIVE', 'CANCELLED'))
