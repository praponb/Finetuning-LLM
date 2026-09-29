SELECT o.id, o.amount, o.order_date, s.city, c.email FROM orders o RIGHT JOIN stores s ON o.store_id = s.id LEFT JOIN customers c ON o.customer_id = c.id;
