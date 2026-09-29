SELECT o.discount, o.order_date, p.order_id, p.method FROM orders o LEFT JOIN payments p ON o.id = p.order_id
