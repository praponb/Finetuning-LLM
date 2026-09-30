SELECT s.country, s.rating, p.launch_date, p.supplier_id, UPPER(p.brand) AS brand_upper FROM suppliers s JOIN products p ON s.id = p.supplier_id
