SELECT
    status,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    CASE WHEN channel IN ('store', 'partner') THEN 'core' ELSE 'other' END AS channel_group,
    ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web
FROM orders
GROUP BY 1, 2, 3
HAVING pct_web < 60
ORDER BY status DESC;
