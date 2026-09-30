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
        ROUND(unit_price * 1.5, 2) AS unit_price_adj,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE sku_id >= 8
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND HOUR(st.moved_at) BETWEEN 7 AND 11
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY unit_price_per_qty DESC NULLS FIRST, id) AS seq_no,
        RANK() OVER w_all_by_unit_price_per_qty_ti AS unit_price_per_qty_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_price_per_qty_ti, 3) AS unit_price_per_qty_percent_rank
    FROM qualified
    WINDOW w_all_by_unit_price_per_qty_ti AS (ORDER BY unit_price_per_qty DESC NULLS FIRST)
)
SELECT id, measure, metric_value
FROM (
    SELECT id, unit_price_per_qty_rank, qty, seq_no
    FROM ranked
) src
UNPIVOT (
    metric_value FOR measure IN (unit_price_per_qty_rank, qty, seq_no)
) unp
ORDER BY id, measure
