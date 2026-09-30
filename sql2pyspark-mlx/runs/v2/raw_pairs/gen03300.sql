SELECT id, store_id, last_restock
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS rnk
    FROM inventory
) AS t
WHERE rnk <= 1
ORDER BY last_restock;
