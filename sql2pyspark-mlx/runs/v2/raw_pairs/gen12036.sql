SELECT channel, MAX(order_date) AS latest_order_date, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
FROM orders
GROUP BY channel
ORDER BY latest_order_date DESC;
