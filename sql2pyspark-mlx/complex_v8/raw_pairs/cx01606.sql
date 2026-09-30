WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_dew', 'op_chan') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE warehouse_id <= 3
      AND move_type NOT IN ('ADJUST', 'INBOUND')
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
),
annotated AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM without_matches
)
-- Subtotals with GROUPING SETS
SELECT
    sku_id,
    warehouse_id,
    COALESCE(move_type, 'ALL') AS move_type,
    COUNT(*) FILTER (WHERE move_type = 'INBOUND') AS n_move_type_inbound,
    PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
FROM annotated
GROUP BY GROUPING SETS ((sku_id, warehouse_id, move_type), (sku_id))
