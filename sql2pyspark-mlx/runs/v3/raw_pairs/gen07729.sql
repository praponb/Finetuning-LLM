SELECT s.country, COUNT(DISTINCT ss.units) AS unique_products
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.store_id IN (3, 5)
GROUP BY s.country
ORDER BY unique_products DESC, country;
