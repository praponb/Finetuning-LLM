WITH Base AS (
    SELECT *
    FROM inventory
    WHERE last_restock < '2024-08-01' AND quantity < 116
),
summary AS (
    SELECT CASE WHEN quantity >= 221 THEN 'high' WHEN quantity >= 60 THEN 'mid' ELSE 'low' END quantity_bucket, MIN(quantity) min_quantity, COUNT(*) row_count
    FROM Base
    GROUP BY CASE WHEN quantity >= 221 THEN 'high' WHEN quantity >= 60 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE row_count >= 8
