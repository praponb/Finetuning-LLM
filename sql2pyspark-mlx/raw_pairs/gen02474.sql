SELECT s.country, COUNT(p.id) AS product_count FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id GROUP BY s.country HAVING product_count >= 1 ORDER BY country
