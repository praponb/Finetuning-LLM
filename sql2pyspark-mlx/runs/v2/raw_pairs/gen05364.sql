SELECT o.status, AVG(s.cost) avg_cost FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id JOIN customers c ON o.customer_id = c.id GROUP BY o.status HAVING avg_cost > 87 ORDER BY status DESC
