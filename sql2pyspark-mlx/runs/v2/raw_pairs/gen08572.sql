SELECT
    s.id,
    s.opened_date,
    s.region,
    t.avg_units,
    t.max_units
FROM stores s
JOIN (SELECT store_id, AVG(units) AS avg_units, MAX(units) AS max_units
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
