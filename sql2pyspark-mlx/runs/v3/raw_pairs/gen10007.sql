SELECT o.amount, s.store_name, s.opened_date, c.name, c.segment FROM orders o LEFT OUTER JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON c.id = o.customer_id;
