SELECT ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_web FROM orders WHERE store_id IN (35, 37) AND order_date < '2023-07-13' ORDER BY pct_web DESC;
