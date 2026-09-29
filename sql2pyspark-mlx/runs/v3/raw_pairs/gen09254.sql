WITH store_stats AS (
    SELECT store_id, SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count
    FROM orders
    GROUP BY store_id
)
SELECT s.opened_date, ss.web_count
FROM stores s
INNER JOIN store_stats ss ON s.id = ss.store_id
