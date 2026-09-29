SELECT s.country, AVG(p.price) AS avg_price
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.supplier_name IS NULL
GROUP BY s.country
