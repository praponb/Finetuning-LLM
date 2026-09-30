SELECT s.id, s.city, COUNT(p.id) product_count
FROM suppliers s
INNER JOIN products p ON p.supplier_id = s.id
GROUP BY s.id, s.city
ORDER BY id DESC, s.city DESC
