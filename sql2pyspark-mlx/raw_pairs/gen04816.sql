SELECT *
FROM (
    SELECT CASE WHEN monthly_fee >= 260 THEN 'high' WHEN monthly_fee >= 111 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket, status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY CASE WHEN monthly_fee >= 260 THEN 'high' WHEN monthly_fee >= 111 THEN 'mid' ELSE 'low' END, status
) AS t
WHERE monthly_fee_range < 42
ORDER BY status DESC;
