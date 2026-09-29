SELECT o.discount, c.is_active, c.signup_date, s.store_name, s.region FROM orders o LEFT OUTER JOIN customers c ON c.id = o.customer_id INNER JOIN stores s ON s.id = o.store_id;
