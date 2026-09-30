SELECT CASE WHEN status IN ('PENDING', 'RETURNED') THEN 'priority' ELSE 'other' END AS status_group, DATE_TRUNC('MONTH', order_date) AS order_month, ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web
FROM orders
WHERE amount >= 940
GROUP BY CASE WHEN status IN ('PENDING', 'RETURNED') THEN 'priority' ELSE 'other' END, DATE_TRUNC('MONTH', order_date)
HAVING ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 59
