SELECT s.id, s.country, t.row_count FROM suppliers s LEFT OUTER JOIN (SELECT supplier_id, COUNT(*) AS row_count FROM products GROUP BY supplier_id) t ON s.id = t.supplier_id ORDER BY s.country
