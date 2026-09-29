SELECT o.order_date, o.store_id, s.opened_date, s.city, c.email FROM orders o LEFT OUTER JOIN stores s ON o.store_id = s.id JOIN customers c ON o.customer_id = c.id;
