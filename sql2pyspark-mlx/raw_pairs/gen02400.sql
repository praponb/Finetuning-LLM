WITH remaining_keys AS (
    SELECT id, store_id
    FROM inventory
    INTERSECT ALL
    SELECT id, store_id
    FROM inventory
    WHERE store_id IN (1, 3, 5, 6)
),
kept_rows AS (
    SELECT s.*
    FROM inventory s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
)
SELECT
    id,
    store_id,
    quantity,
    last_restock
FROM kept_rows
