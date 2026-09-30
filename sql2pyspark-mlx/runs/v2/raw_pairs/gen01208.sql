WITH store_summary AS (
    SELECT store_id, SUM(quantity) AS total_quantity, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.city,
    s.sqft,
    ss.total_quantity,
    ss.avg_quantity
FROM stores s
JOIN store_summary ss ON s.id = ss.store_id
ORDER BY 3 DESC;
