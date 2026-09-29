WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE qty > 99
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id
    FROM base_moves s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM detail_rows_moves
    WHERE move_unit_price < (SELECT MAX(move_unit_price) FROM detail_rows_moves)
),
deduped AS (
    -- Keep the latest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        move_id,
        move_moved_at,
        move_unit_price,
        move_operator,
        move_warehouse_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY qty DESC, id DESC, move_id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(move_unit_price) OVER (PARTITION BY move_operator) AS move_operator_max_move_unit_price,
        NTILE(4) OVER (PARTITION BY warehouse_id, move_type ORDER BY move_moved_at DESC, id, move_id) AS move_moved_at_quartile,
        LEAD(move_moved_at) OVER (PARTITION BY warehouse_id, move_type ORDER BY move_moved_at DESC, id, move_id) AS next_move_moved_at
    FROM deduped
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        move_unit_price + qty AS move_unit_price_plus_qty,
        ROUND(move_moved_at_quartile / NULLIF(move_unit_price, 0), 3) AS move_moved_at_quartile_per_move_unit_price
    FROM ranked
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, metric, value
FROM (
    SELECT id, move_id, move_unit_price_plus_qty, move_moved_at_quartile_per_move_unit_price, move_unit_price
    FROM tagged
) src
UNPIVOT (
    value FOR metric IN (move_unit_price_plus_qty, move_moved_at_quartile_per_move_unit_price, move_unit_price)
) unp
