SELECT s.id, s.city, COUNT(p.id) AS product_count FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id GROUP BY s.id, s.city HAVING COUNT(p.id) >= 5;
