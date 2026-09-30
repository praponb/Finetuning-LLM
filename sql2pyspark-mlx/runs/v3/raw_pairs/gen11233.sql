SELECT s.country, SUM(ss.revenue) AS total_revenue, COUNT(ss.units) AS units_count
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.brand != 'Globex'
GROUP BY s.country;
