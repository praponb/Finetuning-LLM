SELECT stores.id, stores.region, t.revenue_range
FROM stores
LEFT JOIN (SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    WHERE units = 20
    GROUP BY store_id) t ON stores.id = t.store_id
ORDER BY 1 DESC;
