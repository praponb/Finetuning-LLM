SELECT status, COUNT(*) AS num_orders FROM orders GROUP BY status HAVING COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) >= 5;
