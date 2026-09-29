SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions WHERE MONTH(end_date) IN (1, 4, 11)) PIVOT (SUM(monthly_fee) FOR status IN ('EXPIRED', 'ACTIVE'))
