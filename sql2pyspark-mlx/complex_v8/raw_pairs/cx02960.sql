WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        UPPER(TRIM(operator)) AS operator_clean,
        LOWER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
qualified AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.city <> 'Bangkok'
        )
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM qualified s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_clean,
        s.move_type_clean,
        s.move_count,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.unit_price AS move_unit_price
    FROM profiled s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.unit_price < 396.4
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, measure, value
FROM (
    SELECT id, move_id, move_unit_price, unit_price
    FROM line_level_moves
) src
UNPIVOT (
    value FOR measure IN (move_unit_price AS m_move_unit_price, unit_price AS unit_price_v)
) unp
