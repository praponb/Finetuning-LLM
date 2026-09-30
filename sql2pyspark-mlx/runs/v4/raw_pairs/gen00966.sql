SELECT s.id, s.supplier_name, s.city, t.row_count FROM suppliers s JOIN (SELECT supplier_id, COUNT(*) AS row_count FROM products GROUP BY supplier_id) t ON s.id = t.supplier_id WHERE t.row_count >= 3
