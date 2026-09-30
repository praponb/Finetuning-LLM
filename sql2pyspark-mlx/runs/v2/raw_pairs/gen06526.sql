SELECT o.id, o.discount, s.sqft, c.country, c.city FROM orders o LEFT JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON o.customer_id = c.id ORDER BY discount;
