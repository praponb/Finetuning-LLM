WITH src AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (1, 5, 10)
),
agg AS (
    SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM src
    GROUP BY product_id
)
SELECT product_id, avg_quantity
FROM agg
ORDER BY 1
