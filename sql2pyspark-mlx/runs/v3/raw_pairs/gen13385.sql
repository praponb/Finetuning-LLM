SELECT s.city, COUNT(p.id) product_count FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id GROUP BY s.city ORDER BY city;
