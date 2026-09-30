SELECT s.city, t.units_range
FROM stores s
LEFT OUTER JOIN (SELECT store_id, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    WHERE revenue IS NOT NULL
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.units_range > 76
ORDER BY city;
