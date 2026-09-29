SELECT s.city, t.revenue_range
FROM stores s
INNER JOIN (SELECT store_id, MAX(revenue) - MIN(revenue) revenue_range
    FROM store_sales
    WHERE revenue BETWEEN 1700 AND 3900
    GROUP BY store_id) t ON s.id = t.store_id
