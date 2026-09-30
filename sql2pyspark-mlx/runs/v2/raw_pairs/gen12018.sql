SELECT o.id, o.discount, o.customer_id, s.region, s.opened_date, c.is_active FROM orders o LEFT OUTER JOIN stores s ON s.id = o.store_id INNER JOIN customers c ON o.customer_id = c.id;
