WITH valid_moves AS (
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
        moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
),
annotated AS (
    -- Derive helper columns
    SELECT *, YEAR(moved_at) AS moved_at_year
    FROM valid_moves
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_plus_2h,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id
    FROM annotated s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.unit_price BETWEEN 25.2 AND 436.9
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty > 93
        )
      AND s.move_operator <> 'op_ann'
)
-- Subtotals with ROLLUP
SELECT
    sku_id,
    COALESCE(operator, 'ALL') AS operator,
    GROUPING(sku_id) AS is_total_sku_id,
    ROUND(SUM(qty) / NULLIF(SUM(move_unit_price), 0), 3) AS qty_to_move_unit_price_ratio
FROM without_matches
GROUP BY ROLLUP(sku_id, operator)
ORDER BY sku_id NULLS LAST, operator NULLS LAST
