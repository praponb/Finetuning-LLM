SELECT s.country, s.supplier_name, p.category, p.product_name, p.price FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id WHERE s.city IN ('Chiang Mai', 'New York');
