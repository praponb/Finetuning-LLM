SELECT s.id, s.supplier_name, COUNT(p.id) AS product_count
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
GROUP BY s.id, s.supplier_name
ORDER BY s.supplier_name DESC, id
LIMIT 5;
