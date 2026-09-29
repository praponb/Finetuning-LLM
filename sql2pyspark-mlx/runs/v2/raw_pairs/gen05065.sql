SELECT status, channel, COUNT(*) AS num_orders, COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count FROM orders WHERE amount > 720 GROUP BY status, channel ORDER BY status DESC;
