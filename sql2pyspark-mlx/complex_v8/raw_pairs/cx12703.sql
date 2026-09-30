WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE unit_price > 164.1
      AND warehouse_id IS NOT NULL
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.unit_price,
        s.move_type_ref,
        s.qty_bucket,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM recent s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.sku_id IS NOT NULL
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type_ref,
        s.qty_bucket,
        s.move_id,
        s.move_sku_id,
        sk.labels AS sku_labels
    FROM with_children s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type_ref,
    move_id,
    SUBSTRING(TRIM(move_type_ref), 1, 3) AS move_type_ref_prefix
FROM joined_sku
WHERE sku_id IS NOT NULL
ORDER BY id, move_id
