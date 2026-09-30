WITH recent_inventory AS (
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_num
    FROM inventory
    WHERE store_id IN (5, 6) OR quantity = 120
),
agg AS (
    SELECT store_id, MIN(quantity_num) AS min_quantity_num
    FROM recent_inventory
    GROUP BY store_id
)
SELECT *
FROM agg
WHERE min_quantity_num < 27
