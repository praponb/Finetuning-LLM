WITH store_summary AS (
    SELECT store_id, MAX(revenue) AS max_revenue, COUNT(*) AS store_sale_count
    FROM store_sales
    WHERE store_id IN (2, 3, 4)
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.region,
    ss.max_revenue,
    ss.store_sale_count
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY s.store_name, id
LIMIT 20;
