SELECT CASE WHEN method IN ('card', 'promptpay') THEN 'priority' ELSE 'other' END AS method_group, ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash
FROM payments
GROUP BY method_group
HAVING ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 27
ORDER BY pct_cash, method_group
LIMIT 5;
