SELECT
    ss.id,
    ss.units,
    ss.store_id,
    s.city
FROM store_sales ss
RIGHT JOIN stores s ON s.id = ss.store_id
WHERE ss.revenue > 2200
ORDER BY store_id
