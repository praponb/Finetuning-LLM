SELECT s.id, COUNT(p.id) AS product_count FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id WHERE p.launch_date <= '2025-09-01' GROUP BY s.id ORDER BY id DESC;
