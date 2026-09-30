SELECT s.city, COUNT(p.id) AS product_count
FROM suppliers s
INNER JOIN products p ON p.supplier_id = s.id
GROUP BY s.city
ORDER BY city DESC;
