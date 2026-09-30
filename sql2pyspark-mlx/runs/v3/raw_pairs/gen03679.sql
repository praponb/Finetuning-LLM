SELECT id, store_id, quantity
FROM (
    SELECT
        id,
        store_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS rn
    FROM inventory
) AS r
WHERE rn <= 3
ORDER BY quantity DESC;
