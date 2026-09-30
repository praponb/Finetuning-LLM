SELECT o.amount, c.email, c.segment, s.region, s.sqft FROM orders o JOIN customers c ON o.customer_id = c.id LEFT JOIN stores s ON s.id = o.store_id WHERE o.amount <= 1400;
