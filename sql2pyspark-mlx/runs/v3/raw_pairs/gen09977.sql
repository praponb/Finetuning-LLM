SELECT o.id, o.store_id, s.store_name, s.opened_date, c.credit_limit FROM orders o JOIN stores s ON s.id = o.store_id JOIN customers c ON o.customer_id = c.id ORDER BY credit_limit;
