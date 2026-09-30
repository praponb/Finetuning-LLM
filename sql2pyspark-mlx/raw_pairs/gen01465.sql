SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS FIRST, id) AS rn
    FROM inventory
) ranked
WHERE rn = 1
