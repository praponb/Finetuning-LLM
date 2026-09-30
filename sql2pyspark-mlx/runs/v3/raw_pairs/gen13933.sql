SELECT carrier, COUNT(CASE WHEN status = 'LOST' THEN 1 END) AS lost_count FROM shipments WHERE order_id IN (11, 16) GROUP BY 1 ORDER BY lost_count
