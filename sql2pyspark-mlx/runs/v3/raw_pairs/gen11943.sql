SELECT o.id, o.amount, s.store_name, s.sqft, s.city FROM orders o INNER JOIN stores s ON o.store_id = s.id WHERE s.sqft <= 6493 AND s.store_name LIKE 'Mal%';
