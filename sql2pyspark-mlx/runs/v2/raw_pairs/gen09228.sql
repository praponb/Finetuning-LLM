WITH store_summary AS (
    SELECT store_id, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM inventory
    WHERE last_restock > '2024-01-01'
    GROUP BY store_id
)
SELECT s.region, ss.stddev_quantity
FROM stores s
JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.stddev_quantity > 66
ORDER BY s.region DESC;
