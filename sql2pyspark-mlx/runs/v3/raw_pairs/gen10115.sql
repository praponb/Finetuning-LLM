SELECT p.launch_date, p.product_name, s.supplier_name, s.rating FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id ORDER BY rating DESC
