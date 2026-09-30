SELECT DATE_TRUNC('MONTH', order_date) AS order_month, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner
FROM orders
GROUP BY DATE_TRUNC('MONTH', order_date)
ORDER BY pct_partner DESC;
