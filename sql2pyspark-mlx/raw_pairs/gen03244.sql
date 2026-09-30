SELECT
    YEAR(start_date) AS start_year,
    plan,
    CASE WHEN monthly_fee >= 197 THEN 'high' WHEN monthly_fee >= 82 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
GROUP BY YEAR(start_date), plan, CASE WHEN monthly_fee >= 197 THEN 'high' WHEN monthly_fee >= 82 THEN 'mid' ELSE 'low' END;
