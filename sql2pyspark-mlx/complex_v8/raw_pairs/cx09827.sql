WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE sku_id <= 13
),
unique_rows AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_per_unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty_per_unit_price DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
with_parent_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.qty_per_unit_price,
        sk.labels AS sku_labels,
        w.city AS warehouse_city
    FROM unique_rows s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    unit_price,
    qty_per_unit_price,
    sku_labels,
    warehouse_city,
    unit_price + qty AS unit_price_plus_qty,
    CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
FROM with_parent_sku
ORDER BY sku_id DESC, move_type_ref, id
LIMIT 10
