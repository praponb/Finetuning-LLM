SELECT s.city, SUM(ss.units) AS total_units
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY s.city
HAVING total_units >= 170
ORDER BY total_units DESC, city
LIMIT 50
