WITH Src AS (
    SELECT *
    FROM order_items
    WHERE quantity <> 4
),
agg AS (
    SELECT product_id, COUNT(*) AS row_count, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM Src
    GROUP BY product_id
)
SELECT product_id, row_count, avg_quantity
FROM agg
ORDER BY row_count ASC
