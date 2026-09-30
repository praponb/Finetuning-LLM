WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label,
        ABS(unit_price - 160.8) AS unit_price_distance,
        CASE WHEN unit_price >= 293.2 THEN 'large' WHEN unit_price >= 236.5 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 09:00:00'
      AND warehouse_id IS NOT NULL
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_distance,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price,
        st.warehouse_id AS move_warehouse_id
    FROM filtered_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM detail_rows s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, metric, amount
FROM (
    SELECT id, move_id, move_unit_price, unit_price_distance, unit_price
    FROM summarized_rows
) src
UNPIVOT (
    amount FOR metric IN (move_unit_price AS move_unit_price_v, unit_price_distance AS unit_price_distance_v, unit_price AS m_unit_price)
) unp
