WITH store_summary AS (
    SELECT store_id, COUNT(*) cnt
    FROM inventory
    WHERE store_id = 2
    GROUP BY store_id
)
SELECT s.id, s.city, ss.cnt
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
ORDER BY cnt DESC
