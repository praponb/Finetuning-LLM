SELECT
    s.city,
    s.region,
    AVG(i.quantity) AS avg_quantity,
    SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN stores s ON i.store_id = s.id
GROUP BY s.city, s.region
HAVING AVG(i.quantity) >= 210
ORDER BY city, avg_quantity DESC, region
LIMIT 3;
