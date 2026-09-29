WITH store_stats AS (
    SELECT store_id, ROUND(AVG(revenue), 2) AS avg_revenue, COUNT(DISTINCT units) AS unique_products
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    ss.avg_revenue,
    ss.unique_products
FROM stores s
INNER JOIN store_stats ss ON s.id = ss.store_id
ORDER BY ss.unique_products DESC
