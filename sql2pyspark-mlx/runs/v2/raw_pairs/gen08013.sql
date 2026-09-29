SELECT s.city, MAX(ss.units) AS max_units, ROUND(STDDEV(ss.units), 2) AS stddev_units
FROM store_sales ss
JOIN stores s ON s.id = ss.store_id
GROUP BY s.city
ORDER BY max_units DESC
