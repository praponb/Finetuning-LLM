SELECT
    CASE WHEN monthly_fee >= 229 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(DISTINCT plan) AS distinct_plans,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    MAX(monthly_fee) FILTER (WHERE status = 'EXPIRED') AS expired_max_monthly_fee
FROM subscriptions
GROUP BY CASE WHEN monthly_fee >= 229 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING avg_monthly_fee > 0;
