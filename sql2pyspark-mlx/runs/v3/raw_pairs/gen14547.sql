SELECT * FROM (SELECT CASE WHEN monthly_fee >= 202 THEN 'high' WHEN monthly_fee >= 103 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, status, monthly_fee FROM subscriptions)
PIVOT (AVG(monthly_fee) FOR status IN ('ACTIVE', 'CANCELLED', 'EXPIRED', 'PAUSED'))
