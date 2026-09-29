SELECT s.id, s.sqft, t.revenue_count
FROM stores s
INNER JOIN (SELECT store_id, COUNT(revenue) AS revenue_count
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.revenue_count >= 9
