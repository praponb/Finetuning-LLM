SELECT * FROM (SELECT CASE WHEN monthly_fee >= 229 THEN 'high' WHEN monthly_fee >= 50 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, status, monthly_fee FROM subscriptions)
PIVOT (SUM(monthly_fee) FOR status IN ('ACTIVE', 'EXPIRED'))
ORDER BY monthly_fee_bucket;
