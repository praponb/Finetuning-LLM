SELECT
    s.id,
    s.region,
    s.store_name,
    t.revenue_range
FROM stores s
INNER JOIN (SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    WHERE revenue >= 3600
    GROUP BY store_id) t ON s.id = t.store_id
