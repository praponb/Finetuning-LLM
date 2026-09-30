SELECT
    customer_id,
    CASE WHEN monthly_fee >= 188 THEN 'high' WHEN monthly_fee >= 81 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    AVG(monthly_fee) AS avg_monthly_fee,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count
FROM subscriptions
GROUP BY customer_id, CASE WHEN monthly_fee >= 188 THEN 'high' WHEN monthly_fee >= 81 THEN 'mid' ELSE 'low' END
HAVING AVG(monthly_fee) < 150;
