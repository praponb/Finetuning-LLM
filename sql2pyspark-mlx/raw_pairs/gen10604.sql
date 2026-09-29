SELECT * FROM (SELECT status, channel, COUNT(*) AS order_count FROM orders GROUP BY status, channel) AS s WHERE order_count < 7
