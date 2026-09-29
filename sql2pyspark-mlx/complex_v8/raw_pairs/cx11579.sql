WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM stock_moves
    WHERE unit_price >= 200.7
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(qty), 2) AS avg_move_qty,
        MAX(unit_price) AS max_move_unit_price,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_qty,
        st.max_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count
    FROM scoped_moves s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM profiled
    WHERE avg_move_qty > (SELECT AVG(avg_move_qty) FROM profiled)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, max_move_unit_price, unit_price
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (max_move_unit_price, unit_price)
) unp
