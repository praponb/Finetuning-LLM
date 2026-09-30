SELECT s.city, MIN(p.cost) AS min_cost FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id WHERE p.cost > 260 GROUP BY s.city HAVING MIN(p.cost) >= 250 ORDER BY min_cost;
