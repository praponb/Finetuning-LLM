SELECT o.store_id, s.region, s.store_name FROM orders o LEFT OUTER JOIN stores s ON o.store_id = s.id WHERE s.store_name NOT LIKE 'Ma%';
