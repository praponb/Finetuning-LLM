SELECT o.id, o.store_id, c.age, c.credit_limit, s.city FROM orders o RIGHT OUTER JOIN customers c ON o.customer_id = c.id INNER JOIN stores s ON o.store_id = s.id;
