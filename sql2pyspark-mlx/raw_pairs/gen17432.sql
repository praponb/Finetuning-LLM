SELECT o.id, o.order_date, o.status, c.segment FROM orders o LEFT JOIN customers c ON c.id = o.customer_id;
