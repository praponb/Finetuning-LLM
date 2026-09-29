WITH filtered_moves AS (
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
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
    WHERE sku_id <= 13
      AND qty <= 256
),
totals AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        MAX(qty) AS max_qty,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        COUNT(*) AS cnt,
        COUNT(DISTINCT operator) AS distinct_operators,
        MIN(unit_price) AS min_unit_price,
        SUM(qty) FILTER (WHERE move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND')) AS qty_move_type_in_adjust_transfer
    FROM filtered_moves
    GROUP BY warehouse_id
    HAVING COUNT(*) >= 2
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.warehouse_id,
        s.distinct_sku_ids,
        s.cnt,
        s.distinct_operators,
        s.min_unit_price,
        s.qty_move_type_in_adjust_transfer,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM totals s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_details AS (
    -- Join warehouses
    SELECT s.*, w.wh_code AS warehouse_wh_code_2, w.capacity AS warehouse_capacity_2
    FROM enriched_warehouse s
    JOIN warehouses w ON w.id = s.warehouse_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(min_unit_price - AVG(min_unit_price) OVER (PARTITION BY distinct_operators, cnt), 2) AS min_unit_price_vs_distinct_operators_cnt_avg,
        SUM(min_unit_price) OVER (PARTITION BY distinct_sku_ids, cnt ORDER BY qty_move_type_in_adjust_transfer, warehouse_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS min_unit_price_sum_3_prior,
        NTILE(3) OVER w_all_by_distinct_operators AS distinct_operators_tercile,
        COUNT(*) OVER (PARTITION BY distinct_operators, cnt) AS rows_in_distinct_operators_cnt
    FROM with_details
    WINDOW w_all_by_distinct_operators AS (ORDER BY distinct_operators DESC, warehouse_id)
)
SELECT
    warehouse_id,
    cnt,
    distinct_operators,
    min_unit_price,
    qty_move_type_in_adjust_transfer,
    warehouse_capacity,
    warehouse_wh_code,
    warehouse_opened_on,
    warehouse_capacity_2,
    min_unit_price_vs_distinct_operators_cnt_avg,
    min_unit_price_sum_3_prior,
    distinct_operators_tercile,
    rows_in_distinct_operators_cnt
FROM with_windows
ORDER BY min_unit_price_vs_distinct_operators_cnt_avg NULLS LAST
