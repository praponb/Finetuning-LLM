SELECT s.country, COUNT(DISTINCT ss.units) AS unique_products
FROM store_sales ss
RIGHT JOIN products p ON p.id = ss.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY country
LIMIT 1;
