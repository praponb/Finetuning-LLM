WITH recent_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock >= '2023-04-16'
),
store_id_summary AS (
    SELECT store_id, MIN(quantity) min_quantity, ROUND(STDDEV(quantity), 2) stddev_quantity
    FROM recent_inventory
    GROUP BY store_id
)
SELECT store_id, min_quantity
FROM store_id_summary
ORDER BY min_quantity DESC, store_id DESC
LIMIT 20
