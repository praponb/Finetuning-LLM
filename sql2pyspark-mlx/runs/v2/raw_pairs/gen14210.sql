SELECT p.supplier_id, p.brand, AVG(ss.units) AS avg_units
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.supplier_id, p.brand
HAVING AVG(ss.units) > 19
