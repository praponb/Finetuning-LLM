SELECT s.id, s.supplier_name, COUNT(p.id) product_count
FROM suppliers s
INNER JOIN products p ON p.supplier_id = s.id
GROUP BY s.id, s.supplier_name
HAVING COUNT(p.id) > 3
ORDER BY s.supplier_name, product_count;
