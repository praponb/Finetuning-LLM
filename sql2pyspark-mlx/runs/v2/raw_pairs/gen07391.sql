WITH store_summary AS (
    SELECT store_id, AVG(quantity) AS avg_quantity
    FROM inventory
    WHERE product_id IN (2, 6, 7, 10)
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    s.city,
    ss.avg_quantity
FROM stores s
JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.avg_quantity >= 140;
