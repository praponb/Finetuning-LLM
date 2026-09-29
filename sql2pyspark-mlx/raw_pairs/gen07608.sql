WITH src AS (
    SELECT *
    FROM inventory
    WHERE quantity BETWEEN 65 AND 221 OR store_id = 2
),
summary AS (
    SELECT store_id, AVG(quantity) AS avg_quantity
    FROM src
    GROUP BY store_id
)
SELECT *
FROM summary
ORDER BY avg_quantity
