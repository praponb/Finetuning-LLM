WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND warehouse_id <= 5
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price,
        s.least_unit_price_qty,
        s.moved_at_plus_7d,
        s.operator_move_type_label,
        sk.category AS sku_category
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_qty
    FROM enriched_sku s
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        unit_price + least_unit_price_qty AS unit_price_plus_least_unit_price_qty
    FROM with_counts
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price, 2) OVER w_sku_category_by_max_move_qty AS next2_unit_price,
        NTILE(3) OVER w_all_by_max_move_qty AS max_move_qty_tercile,
        NTH_VALUE(max_move_qty, 2) OVER (PARTITION BY sku_category ORDER BY max_move_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_move_qty,
        MIN(unit_price) OVER (PARTITION BY sku_id) AS sku_id_min_unit_price,
        SUM(least_unit_price_qty) OVER (ORDER BY max_move_qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_unit_price_qty,
        FIRST_VALUE(least_unit_price_qty) OVER (ORDER BY max_move_qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_least_unit_price_qty
    FROM annotated
    WINDOW w_sku_category_by_max_move_qty AS (PARTITION BY sku_category ORDER BY max_move_qty, id), w_all_by_max_move_qty AS (ORDER BY max_move_qty ASC, id)
)
-- Subtotals with ROLLUP
SELECT
    max_move_qty_tercile,
    sku_category,
    GROUPING(max_move_qty_tercile) AS is_total_max_move_qty_tercile,
    ROUND(SUM(max_move_qty_tercile) / NULLIF(SUM(max_move_qty), 0), 3) AS max_move_qty_tercile_to_max_move_qty_ratio
FROM with_windows
GROUP BY ROLLUP(max_move_qty_tercile, sku_category)
