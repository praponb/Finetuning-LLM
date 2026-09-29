WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price + qty AS unit_price_plus_qty,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d
    FROM stock_moves
    WHERE move_type NOT IN ('ADJUST', 'INBOUND')
      AND qty BETWEEN 99 AND 332
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(unit_price_plus_qty) OVER (ORDER BY unit_price_per_qty ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_price_plus_qty_avg_3_prior,
        ROUND(AVG(unit_price_plus_qty) OVER (PARTITION BY sku_id, operator ORDER BY unit_price_per_qty DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_plus_qty_avg_1_prior
    FROM scoped
),
move_type_stats AS (
    -- Avg unit_price_plus_qty per move_type
    SELECT move_type, AVG(unit_price_plus_qty) AS avg_unit_price_plus_qty
    FROM sequenced
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price_plus_qty,
        ROUND(s.unit_price_plus_qty - g.avg_unit_price_plus_qty, 2) AS unit_price_plus_qty_minus_avg
    FROM sequenced s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price_plus_qty > g.avg_unit_price_plus_qty
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'OUTBOUND'
              AND st.sku_id IS NOT NULL
        )
      AND s.unit_price_per_qty BETWEEN 3.0 AND 8.0
)
-- Final projection
SELECT
    id,
    move_type,
    qty,
    moved_at_plus_7d,
    unit_price_plus_qty_avg_1_prior,
    avg_unit_price_plus_qty,
    unit_price_plus_qty_minus_avg,
    CONCAT_WS('|', operator, move_type) AS operator_move_type_label,
    CASE WHEN operator IN ('op_ann', 'op_eve') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
FROM with_activity
ORDER BY avg_unit_price_plus_qty DESC
