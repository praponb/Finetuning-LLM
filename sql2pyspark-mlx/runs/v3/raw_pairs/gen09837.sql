WITH store_summary AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE store_id IN (2, 3, 5, 6)
    GROUP BY store_id
)
SELECT
    s.sqft,
    s.region,
    ss.quantity_range,
    ss.stddev_quantity
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY quantity_range DESC;
