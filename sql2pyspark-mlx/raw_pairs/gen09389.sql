WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.store_id,
    s.quantity,
    s.last_restock,
    s.row_num,
    (SELECT MIN(i2.quantity) FROM inventory i2 WHERE i2.store_id = s.store_id) AS min_inventory_quantity
FROM deduped s
ORDER BY row_num DESC
