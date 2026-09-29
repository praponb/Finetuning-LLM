SELECT
    s.id,
    s.opened_date,
    t.revenue_range,
    t.total_units
FROM stores s
INNER JOIN (SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range, SUM(units) AS total_units
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY s.id
