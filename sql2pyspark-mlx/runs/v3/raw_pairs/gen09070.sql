SELECT s.city, SUM(p.cost) AS total_cost
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city
HAVING SUM(p.cost) >= 3100
