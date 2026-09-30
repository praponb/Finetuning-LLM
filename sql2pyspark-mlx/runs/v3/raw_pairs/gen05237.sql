SELECT p.id, p.order_id, o.order_date, o.channel FROM payments p RIGHT JOIN orders o ON p.order_id = o.id WHERE o.amount >= 1170 ORDER BY order_date, o.channel DESC
