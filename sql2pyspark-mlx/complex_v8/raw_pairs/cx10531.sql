WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM stock_moves
    WHERE sku_id <= 5
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM cleaned s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        moved_at_year,
        warehouse_opened_on,
        NTILE(3) OVER w_warehouse_opened_on_month_by AS qty_tercile,
        ROUND(qty - AVG(qty) OVER (PARTITION BY MONTH(warehouse_opened_on)), 2) AS qty_vs_warehouse_opened_on_month_avg,
        qty - LAG(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY qty DESC, id) AS qty_change,
        NTILE(5) OVER w_warehouse_id_by_moved_at_yea AS moved_at_year_quintile,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY MONTH(warehouse_opened_on))) AS distinct_move_type_in_warehouse_opened_on_month,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY warehouse_id ORDER BY moved_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        MAX(unit_price) OVER w_warehouse_opened_on_month AS warehouse_opened_on_month_max_unit_price,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        FIRST_VALUE(unit_price) OVER (PARTITION BY warehouse_id ORDER BY moved_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        SUM(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY warehouse_opened_on ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior
    FROM combined_details_warehouse
    WINDOW w_warehouse_opened_on_month_by AS (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on) ORDER BY qty DESC, id), w_warehouse_id_by_moved_at_yea AS (PARTITION BY warehouse_id ORDER BY moved_at_year ASC, id), w_warehouse_opened_on_month AS (PARTITION BY MONTH(warehouse_opened_on))
)
SELECT
    id,
    sku_id,
    moved_at,
    unit_price,
    moved_at_year,
    warehouse_opened_on,
    qty_tercile,
    qty_vs_warehouse_opened_on_month_avg,
    moved_at_year_quintile,
    second_unit_price,
    first_unit_price,
    ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(moved_at_year)) / 3600, 1) AS hours_moved_at_year_to_moved_at
FROM with_trends
ORDER BY qty_vs_warehouse_opened_on_month_avg, qty_tercile DESC
