SELECT c.segment, COUNT(o.id) AS order_count FROM customers c LEFT OUTER JOIN orders o ON c.id = o.customer_id GROUP BY c.segment ORDER BY segment LIMIT 10;
