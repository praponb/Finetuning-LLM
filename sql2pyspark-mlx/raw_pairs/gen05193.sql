SELECT s.rating, s.country, s.city, p.launch_date, p.product_name FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id
