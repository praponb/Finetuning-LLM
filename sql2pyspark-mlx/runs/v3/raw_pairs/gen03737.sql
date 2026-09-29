SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions WHERE customer_id = 33) PIVOT (COUNT(monthly_fee) FOR status IN ('PAUSED', 'ACTIVE'))
