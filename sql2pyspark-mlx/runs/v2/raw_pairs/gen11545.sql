SELECT s.region, s.city, MAX(ss.units) AS max_units
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
GROUP BY s.region, s.city
ORDER BY max_units, city
