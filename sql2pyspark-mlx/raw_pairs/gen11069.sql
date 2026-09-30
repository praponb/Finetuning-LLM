SELECT store_id, min_last_restock, cnt
FROM (
    SELECT store_id, MIN(last_restock) AS min_last_restock, COUNT(*) AS cnt
    FROM inventory
    GROUP BY store_id
) AS agg
WHERE cnt < 10
