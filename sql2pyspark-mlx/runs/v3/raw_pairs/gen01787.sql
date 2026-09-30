SELECT o.customer_id, o.amount, c.segment, s.sqft FROM orders o JOIN customers c ON o.customer_id = c.id JOIN stores s ON o.store_id = s.id
