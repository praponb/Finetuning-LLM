SELECT store_id, max_quantity, stddev_quantity
FROM (
    SELECT store_id, MAX(quantity) AS max_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    GROUP BY store_id
) AS agg
WHERE max_quantity < 90
ORDER BY store_id, stddev_quantity
LIMIT 1
