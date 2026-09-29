WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        MONTH(moved_at) AS moved_at_month,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 10:00:00'
      AND move_type = 'ADJUST'
),
deduped AS (
    -- Keep the latest row per move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        moved_at_month,
        unit_price_per_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price DESC, id DESC) AS rn
        FROM filtered_moves
    ) ranked
    WHERE rn = 1
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at_2
    FROM deduped s
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator = 'op_chan'
        )
      AND s.last_moved_at_2 >= TIMESTAMP '2025-01-25 09:00:00'
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    move_type,
    GROUPING(warehouse_id) AS is_total_warehouse_id,
    COUNT(*) FILTER (WHERE move_type NOT IN ('OUTBOUND', 'INBOUND')) AS n_move_type_not_in_outbound_in
FROM qualified
GROUP BY ROLLUP(warehouse_id, move_type)
