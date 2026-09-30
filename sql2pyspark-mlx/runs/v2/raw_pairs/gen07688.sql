SELECT p.id, p.cost, p.product_name, p.category, s.supplier_name FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.price < 820;
