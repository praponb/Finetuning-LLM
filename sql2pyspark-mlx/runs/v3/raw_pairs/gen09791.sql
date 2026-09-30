SELECT s.region, t.row_count
FROM stores s
LEFT OUTER JOIN (SELECT store_id, COUNT(*) AS row_count
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.row_count >= 9
ORDER BY row_count DESC
