WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(unit_price - 209.4) AS unit_price_distance,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj
    FROM stock_moves
    WHERE sku_id >= 18
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(qty, 0) AS qty_filled
    FROM filtered_moves
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(moved_at) AS moved_at_year,
        operator,
        warehouse_id,
        COUNT(*) FILTER (WHERE move_type = 'TRANSFER') AS n_move_type_transfer,
        SUM(qty) AS total_qty,
        COUNT(*) AS n_rows,
        SUM(qty) FILTER (WHERE operator <> 'op_chan') AS qty_operator_op_chan
    FROM labeled
    GROUP BY 1, 2, 3
),
with_parent_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.moved_at_year,
        s.operator,
        s.warehouse_id,
        s.n_move_type_transfer,
        s.total_qty,
        s.n_rows,
        s.qty_operator_op_chan,
        w.wh_code AS warehouse_wh_code
    FROM per_group s
    JOIN warehouses w ON w.id = s.warehouse_id
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT s.*, w.wh_code AS warehouse_wh_code_2
    FROM with_parent_warehouse s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(warehouse_wh_code_2), ' ', '_')) AS warehouse_wh_code_2_slug
    FROM enriched_warehouse
)
-- Final projection
SELECT
    moved_at_year,
    total_qty,
    n_rows,
    qty_operator_op_chan,
    warehouse_wh_code,
    warehouse_wh_code_2,
    warehouse_wh_code_2_slug
FROM scored
