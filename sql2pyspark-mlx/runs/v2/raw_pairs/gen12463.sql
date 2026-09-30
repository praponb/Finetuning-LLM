SELECT id, store_id, quantity
FROM (
    SELECT
        id,
        store_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) pos
    FROM inventory
) t
WHERE pos <= 1
ORDER BY quantity DESC;
