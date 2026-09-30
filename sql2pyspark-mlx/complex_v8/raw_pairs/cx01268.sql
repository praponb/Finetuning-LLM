WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id >= 17
      AND warehouse_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LAG(qty, 1, 0) OVER (ORDER BY qty DESC NULLS FIRST, id) AS prev_qty,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER w_operator_sku_id_by_qty)) / 60, 1) AS minutes_since_prev_moved_at
    FROM scoped_moves
    WINDOW w_operator_sku_id_by_qty AS (PARTITION BY operator, sku_id ORDER BY qty, id)
),
labeled AS (
    -- Add calculated columns
    SELECT *, FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM with_trends
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.prev_qty,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM labeled s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    sku_id,
    moved_at,
    unit_price,
    prev_qty,
    move_id,
    move_warehouse_id,
    move_moved_at
FROM exploded_detail_moves
WHERE unit_price <= 396.4
ORDER BY move_moved_at DESC, id DESC
