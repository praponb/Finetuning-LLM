SELECT s.city, s.country, p.product_name FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id WHERE NOT (s.country != 'DE' AND s.city = 'Singapore');
