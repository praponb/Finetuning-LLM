WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND operator NOT IN ('op_ann', 'op_bee')
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_unit_price,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at
    FROM prepared_moves s
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', first_moved_at) AS days_since_first_moved_at
    FROM with_counts
    WHERE warehouse_id >= 4
)
SELECT *
FROM (
    SELECT warehouse_id, sku_id, operator_or_unknown, unit_price
    FROM labeled
) src
PIVOT (
    MAX(unit_price) AS max_v, SUM(unit_price) AS sum_v
    FOR operator_or_unknown IN ('op_dew', 'op_bee')
)
