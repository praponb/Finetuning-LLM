WITH cleaned AS (
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
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE sku_id >= 15
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(qty) OVER (PARTITION BY operator, move_type ORDER BY unit_price_per_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        MAX(unit_price) OVER (ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator, move_type), 2) AS operator_move_type_avg_unit_price,
        COUNT(*) OVER (PARTITION BY operator, move_type) AS rows_in_operator_move_type,
        NTILE(10) OVER (ORDER BY qty DESC NULLS FIRST, id) AS qty_decile,
        SUM(unit_price_per_qty) OVER (PARTITION BY operator, move_type) AS operator_move_type_sum_unit_price_per_qty
    FROM cleaned
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_per_qty,
        s.max_unit_price_so_far,
        s.rows_in_operator_move_type,
        s.qty_decile,
        s.operator_move_type_sum_unit_price_per_qty,
        w.wh_code AS warehouse_wh_code
    FROM with_trends s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city = 'Singapore'
),
with_details_2 AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_per_qty,
        s.max_unit_price_so_far,
        s.rows_in_operator_move_type,
        s.warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM with_details s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.opened_on >= DATE '2024-11-25'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price_per_qty,
        rows_in_operator_move_type,
        warehouse_wh_code,
        COALESCE(max_unit_price_so_far, 0.0) AS max_unit_price_so_far_filled
    FROM with_details_2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY warehouse_id, move_type ORDER BY rows_in_operator_move_type NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, move_type), 2) AS unit_price_vs_warehouse_id_move_type_avg,
        COUNT(*) OVER (PARTITION BY warehouse_id, move_type) AS rows_in_warehouse_id_move_type,
        LAST_VALUE(rows_in_operator_move_type) IGNORE NULLS OVER (PARTITION BY move_type, warehouse_id ORDER BY max_unit_price_so_far_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rows_in_operator_move_type,
        NTILE(3) OVER (PARTITION BY move_type, warehouse_id ORDER BY max_unit_price_so_far_filled DESC, id) AS max_unit_price_so_far_filled_tercile
    FROM tagged
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter,
        COALESCE(qty, 0) AS qty_filled
    FROM sequenced
)
SELECT
    id,
    qty,
    unit_price,
    unit_price_per_qty,
    rows_in_operator_move_type,
    max_unit_price_so_far_filled,
    unit_price_vs_warehouse_id_move_type_avg,
    last_known_rows_in_operator_move_type,
    max_unit_price_so_far_filled_tercile,
    moved_at_quarter,
    qty_filled,
    CONCAT_WS('|', warehouse_wh_code, move_type) AS warehouse_wh_code_move_type_label
FROM tagged_2
ORDER BY moved_at_quarter DESC, unit_price_per_qty DESC NULLS FIRST, id
LIMIT 10
