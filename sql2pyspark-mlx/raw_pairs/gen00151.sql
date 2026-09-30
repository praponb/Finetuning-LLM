SELECT CASE WHEN plan IN ('basic', 'standard') THEN 'priority' ELSE 'other' END AS plan_group, SUM(monthly_fee) AS total_monthly_fee, ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
WHERE monthly_fee >= 190
GROUP BY CASE WHEN plan IN ('basic', 'standard') THEN 'priority' ELSE 'other' END
ORDER BY total_monthly_fee DESC;
