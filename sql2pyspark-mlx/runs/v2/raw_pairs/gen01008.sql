SELECT
    s.id,
    s.opened_date,
    s.city,
    t.units_count,
    t.stddev_units
FROM stores s
LEFT JOIN (SELECT store_id, COUNT(units) AS units_count, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
