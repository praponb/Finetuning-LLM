SELECT p.id, p.category, p.launch_date, p.cost, s.supplier_name, s.rating FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.product_name NOT LIKE 'USB%';
