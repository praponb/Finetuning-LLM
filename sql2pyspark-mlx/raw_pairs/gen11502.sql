SELECT p.product_name, p.brand, p.cost, s.supplier_name FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY p.brand;
