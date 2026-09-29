SELECT s.id, s.region, t.avg_revenue
FROM stores s
INNER JOIN (SELECT store_id, AVG(revenue) avg_revenue
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.avg_revenue >= 2200
ORDER BY s.region, id
LIMIT 20;
