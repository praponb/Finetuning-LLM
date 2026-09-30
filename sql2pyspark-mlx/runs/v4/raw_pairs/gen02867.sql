WITH src AS (
    SELECT *
    FROM orders
    WHERE store_id IS NOT NULL OR channel != 'mobile'
),
Agg AS (
    SELECT store_id, SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count
    FROM src
    GROUP BY store_id
)
SELECT store_id, completed_count
FROM Agg
WHERE completed_count > 68
ORDER BY store_id DESC;
