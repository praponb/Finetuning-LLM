SELECT p.supplier_id, p.launch_date, p.product_name, s.supplier_name, s.country FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id
