SELECT
    id,
    store_id,
    last_restock,
    quantity
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rn
    FROM inventory
) AS ranked
WHERE rn <= 1
ORDER BY id DESC;
