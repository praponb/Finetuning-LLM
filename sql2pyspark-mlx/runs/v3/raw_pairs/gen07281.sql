SELECT * FROM (SELECT customer_id, status, monthly_fee FROM subscriptions)
PIVOT (AVG(monthly_fee) FOR status IN ('ACTIVE', 'CANCELLED', 'PAUSED'))
ORDER BY customer_id;
