WITH base AS (
    SELECT *
    FROM inventory
    WHERE last_restock <= '2024-11-05' AND quantity BETWEEN 79 AND 152
),
store_id_summary AS (
    SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM base
    GROUP BY store_id
)
SELECT store_id, stddev_quantity
FROM store_id_summary
WHERE stddev_quantity > 80
ORDER BY stddev_quantity DESC, store_id;
