SELECT p.order_id, o.order_date, o.customer_id FROM payments p LEFT JOIN orders o ON o.id = p.order_id;
