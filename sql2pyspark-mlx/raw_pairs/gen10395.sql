SELECT s.* FROM suppliers s WHERE EXISTS (SELECT 1 FROM products x WHERE x.supplier_id = s.id AND YEAR(x.launch_date) >= 2023)
