SELECT s.region, s.city, SUM(ss.units) AS total_units
FROM store_sales ss
RIGHT OUTER JOIN stores s ON ss.store_id = s.id
LEFT OUTER JOIN products p ON ss.product_id = p.id
GROUP BY s.region, s.city
HAVING SUM(ss.units) > 240
ORDER BY total_units ASC
