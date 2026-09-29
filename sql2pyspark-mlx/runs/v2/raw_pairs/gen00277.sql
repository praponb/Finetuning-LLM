WITH store_summary AS (
    SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.region,
    s.city,
    ss.avg_quantity,
    ss.max_quantity
FROM stores s
LEFT OUTER JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.max_quantity > 220;
