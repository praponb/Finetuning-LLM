SELECT p.brand, SUM(ss.units) AS total_units
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.brand
HAVING total_units >= 160
ORDER BY brand DESC, total_units DESC
LIMIT 10
