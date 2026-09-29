SELECT p.status, p.order_id, o.status order_status FROM payments p LEFT JOIN orders o ON p.order_id = o.id;
