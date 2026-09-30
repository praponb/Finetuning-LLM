WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE (move_type NOT IN ('ADJUST', 'OUTBOUND') OR qty <= 288)
),
combined_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        w.opened_on AS warehouse_opened_on,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(moved_at) OVER (PARTITION BY warehouse_id, operator ORDER BY sku_unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY qty DESC, id) AS qty_rank,
        FIRST_VALUE(qty) IGNORE NULLS OVER (ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        MIN(qty) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        DATEDIFF(warehouse_opened_on, LAG(warehouse_opened_on) OVER (PARTITION BY sku_category, warehouse_id ORDER BY sku_unit_cost DESC, id)) AS days_since_prev_warehouse_opened_on,
        COUNT(*) OVER (PARTITION BY MONTH(moved_at)) AS rows_in_moved_at_month,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY MONTH(moved_at)), 0), 4) AS qty_share_of_moved_at_month,
        FIRST_VALUE(sku_unit_cost) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_unit_cost,
        ROUND(PERCENT_RANK() OVER w_warehouse_opened_on_month_by_3, 3) AS qty_percent_rank,
        ROUND(AVG(qty) OVER w_operator_warehouse_opened_on, 2) AS operator_warehouse_opened_on_month_avg_qty,
        NTH_VALUE(qty, 2) OVER (PARTITION BY sku_id, warehouse_id ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty
    FROM combined_details
    WINDOW w_warehouse_opened_on_month_by_3 AS (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY qty DESC), w_operator_warehouse_opened_on AS (PARTITION BY operator, DATE_TRUNC('MONTH', warehouse_opened_on))
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        warehouse_opened_on,
        sku_category,
        sku_unit_cost,
        second_warehouse_opened_on,
        running_count,
        last_moved_at,
        first_known_qty,
        min_qty_so_far,
        days_since_prev_warehouse_opened_on,
        rows_in_moved_at_month,
        qty_share_of_moved_at_month,
        first_sku_unit_cost,
        qty_percent_rank,
        operator_warehouse_opened_on_month_avg_qty,
        second_qty
    FROM windowed
    WHERE qty_rank <= 5
)
SELECT
    id,
    warehouse_id,
    qty,
    moved_at,
    operator,
    warehouse_opened_on,
    sku_category,
    last_moved_at,
    first_known_qty,
    min_qty_so_far,
    days_since_prev_warehouse_opened_on,
    rows_in_moved_at_month,
    qty_share_of_moved_at_month,
    qty_percent_rank,
    operator_warehouse_opened_on_month_avg_qty
FROM first_rows
ORDER BY operator DESC
