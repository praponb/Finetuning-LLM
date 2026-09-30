SELECT s.city, SUM(p.cost) AS total_cost FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE p.cost BETWEEN 260 AND 290 GROUP BY s.city ORDER BY city, total_cost;
