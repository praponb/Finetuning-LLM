WITH src AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (1, 2, 3)
),
Summary AS (
    SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM src
    GROUP BY store_id
)
SELECT *
FROM Summary
