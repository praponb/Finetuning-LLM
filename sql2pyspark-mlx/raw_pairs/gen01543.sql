SELECT * FROM (SELECT customer_id, status, monthly_fee FROM subscriptions) src PIVOT (AVG(monthly_fee) AS amount, COUNT(1) AS n FOR status IN ('PAUSED' AS paused, 'EXPIRED' AS expired))
