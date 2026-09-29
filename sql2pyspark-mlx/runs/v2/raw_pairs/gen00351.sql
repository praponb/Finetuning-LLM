SELECT o.status, MAX(p.amount) AS max_amount FROM payments p JOIN orders o ON o.id = p.order_id WHERE o.status = 'SHIPPED' GROUP BY o.status ORDER BY status;
