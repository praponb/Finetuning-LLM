WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label
    FROM stock_moves
    WHERE unit_price < 399.6
      AND sku_id >= 8
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at_label,
        sk.is_active AS sku_is_active,
        sk.sku_code
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at_label,
        sku_is_active,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type) AS distinct_warehouse_id_in_move_type,
        NTH_VALUE(qty, 2) OVER (PARTITION BY sku_id ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM joined_sku
    WINDOW w_move_type AS (PARTITION BY move_type)
)
SELECT
    id,
    warehouse_id,
    sku_is_active,
    distinct_warehouse_id_in_move_type,
    second_qty,
    ROUND(qty / NULLIF(second_qty, 0), 3) AS qty_per_second_qty,
    GREATEST(second_qty, qty) AS greatest_second_qty_qty
FROM windowed
WHERE sku_is_active = FALSE
ORDER BY qty_per_second_qty DESC NULLS FIRST
