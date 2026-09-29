SELECT
    YEAR(start_date) AS start_year,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values,
    ROUND((100 * SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_active,
    SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee
FROM subscriptions
WHERE monthly_fee < 220
GROUP BY YEAR(start_date)
ORDER BY enterprise_monthly_fee DESC;
