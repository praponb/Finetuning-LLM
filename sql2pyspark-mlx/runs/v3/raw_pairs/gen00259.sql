SELECT s.rating, s.country, s.city, p.brand, p.launch_date, p.product_name FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
