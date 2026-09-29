SELECT p.id, p.status, p.order_id, o.amount FROM payments p INNER JOIN orders o ON o.id = p.order_id WHERE o.amount <= 1040 ORDER BY order_id
