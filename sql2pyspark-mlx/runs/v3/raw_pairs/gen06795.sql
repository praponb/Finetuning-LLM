SELECT s.country, MAX(p.cost) - MIN(p.cost) cost_range, COUNT(p.price) price_count
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
HAVING price_count > 3
ORDER BY cost_range DESC, country
LIMIT 3
