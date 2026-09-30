SELECT o.id, o.order_date, o.store_id, s.city, c.segment FROM orders o LEFT JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON o.customer_id = c.id
