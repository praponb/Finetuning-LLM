SELECT o.id, o.store_id, s.sqft, s.region, c.country FROM orders o INNER JOIN stores s ON o.store_id = s.id RIGHT JOIN customers c ON c.id = o.customer_id;
