SELECT channel, status, MIN(order_date) AS min_order_date, SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count FROM orders GROUP BY channel, status ORDER BY status;
