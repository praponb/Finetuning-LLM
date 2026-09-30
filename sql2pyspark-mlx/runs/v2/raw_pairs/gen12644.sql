SELECT s.id, s.country, COUNT(p.id) AS product_count FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id GROUP BY s.id, s.country ORDER BY country;
