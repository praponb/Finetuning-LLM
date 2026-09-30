SELECT *
FROM (
    SELECT store_id, COUNT(*) row_count
    FROM inventory
    GROUP BY store_id
) x
WHERE row_count > 4
ORDER BY row_count DESC
