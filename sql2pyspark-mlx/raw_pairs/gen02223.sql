WITH base AS (
    SELECT *
    FROM inventory
    WHERE MONTH(last_restock) IN (3, 8, 12)
),
summary AS (
    SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM base
    GROUP BY store_id
)
SELECT store_id, stddev_quantity
FROM summary
WHERE stddev_quantity < 41
ORDER BY stddev_quantity DESC;
