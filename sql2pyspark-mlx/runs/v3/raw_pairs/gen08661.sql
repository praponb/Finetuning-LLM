SELECT id, store_id, last_restock
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
    WHERE last_restock <= '2025-05-21'
) AS t
WHERE pos = 1;
