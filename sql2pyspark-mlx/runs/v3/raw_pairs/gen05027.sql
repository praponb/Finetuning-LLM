SELECT s.rating, s.city, s.country, p.launch_date FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
