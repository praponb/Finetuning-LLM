WITH store_summary AS (
    SELECT store_id, COUNT(DISTINCT units) AS unique_stores
    FROM store_sales
    GROUP BY store_id
)
SELECT s.id, s.region, ss.unique_stores
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY region DESC, unique_stores ASC
