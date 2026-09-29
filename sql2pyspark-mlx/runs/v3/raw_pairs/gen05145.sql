SELECT p.brand, p.cost, p.product_name, s.city, s.country FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE s.city IN ('Chiang Mai', 'London') ORDER BY product_name;
