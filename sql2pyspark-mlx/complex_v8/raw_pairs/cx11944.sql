WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CASE WHEN move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price,
        CASE WHEN unit_price >= 298.9 THEN 'large' WHEN unit_price >= 111.7 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM stock_moves
    WHERE unit_price <= 313.8
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(greatest_qty_unit_price, unit_price) AS greatest_greatest_qty_unit_price_unit_price,
        ABS(greatest_qty_unit_price - 250.5) AS greatest_qty_unit_price_distance,
        ROUND(greatest_qty_unit_price * 1.5, 2) AS greatest_qty_unit_price_adj
    FROM valid
    WHERE greatest_qty_unit_price > 75.8
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_qty,
        (SELECT ROUND(AVG(st.unit_price), 2) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS avg_move_unit_price
    FROM derived s
),
grouped AS (
    -- Group-level totals
    SELECT
        move_type,
        TO_DATE(last_moved_at) AS last_moved_at_day,
        move_type_group,
        ROUND(SUM(greatest_qty_unit_price_distance), 2) AS total_greatest_qty_unit_price_distance,
        ROUND(SUM(greatest_qty_unit_price) / NULLIF(SUM(greatest_qty_unit_price_distance), 0), 3) AS greatest_qty_unit_price_to_greatest_qty_unit_pri,
        COUNT(*) AS cnt,
        MAX(max_move_qty) AS max_max_move_qty
    FROM with_counts
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Label and score rows
    SELECT *, DATE_ADD(last_moved_at_day, 7) AS last_moved_at_day_plus_7d
    FROM grouped
)
SELECT DISTINCT move_type_group
FROM labeled
ORDER BY move_type_group DESC
