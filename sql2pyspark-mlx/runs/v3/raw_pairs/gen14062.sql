SELECT s.region, t.stddev_revenue, t.revenue_range
FROM stores s
INNER JOIN (SELECT store_id, ROUND(STDDEV(revenue), 2) stddev_revenue, MAX(revenue) - MIN(revenue) revenue_range
    FROM store_sales
    WHERE product_id IN (3, 6, 8)
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.stddev_revenue > 38
ORDER BY stddev_revenue DESC
