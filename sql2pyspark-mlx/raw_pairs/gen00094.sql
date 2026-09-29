SELECT p.id, p.status, o.order_date, o.channel FROM payments p RIGHT OUTER JOIN orders o ON p.order_id = o.id ORDER BY 4
