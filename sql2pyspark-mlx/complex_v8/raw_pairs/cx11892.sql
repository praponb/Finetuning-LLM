WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER w_all_by_qty AS qty_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type, operator ORDER BY unit_price), 3) AS unit_price_percent_rank,
        MAX(unit_price) OVER w_warehouse_id_move_type AS warehouse_id_move_type_max_unit_price,
        RANK() OVER (ORDER BY qty NULLS LAST) AS qty_rank,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        DENSE_RANK() OVER (ORDER BY greatest_qty_unit_price DESC, id) AS greatest_qty_unit_price_dense_rank,
        LAST_VALUE(qty) OVER (PARTITION BY move_type, operator ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        DENSE_RANK() OVER w_all_by_greatest_qty_unit_pri_2 AS greatest_qty_unit_price_dense_rank_2,
        LAG(unit_price) OVER (PARTITION BY MONTH(moved_at), warehouse_id ORDER BY qty, id) AS prev_unit_price,
        NTILE(3) OVER (PARTITION BY MONTH(moved_at), warehouse_id ORDER BY qty, id) AS qty_tercile
    FROM cleaned
    WINDOW w_all_by_qty AS (ORDER BY qty NULLS LAST, id), w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type), w_all_by_greatest_qty_unit_pri_2 AS (ORDER BY greatest_qty_unit_price DESC)
),
scored AS (
    -- Label and score rows
    SELECT *, FLOOR(qty_tercile / 2) * 2 AS qty_tercile_bucket
    FROM with_trends
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        qty,
        unit_price,
        qty_decile,
        unit_price_percent_rank,
        warehouse_id_move_type_max_unit_price,
        rows_in_sku_id,
        greatest_qty_unit_price_dense_rank,
        last_qty,
        greatest_qty_unit_price_dense_rank_2,
        prev_unit_price,
        qty_tercile_bucket,
        LAST_VALUE(prev_unit_price) OVER (PARTITION BY sku_id, operator ORDER BY qty_tercile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY qty_tercile_bucket)) AS distinct_warehouse_id_in_qty_tercile_bucket
    FROM scored
    WINDOW w_qty_tercile_bucket AS (PARTITION BY qty_tercile_bucket)
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    unit_price,
    qty_decile,
    unit_price_percent_rank,
    warehouse_id_move_type_max_unit_price,
    greatest_qty_unit_price_dense_rank,
    last_qty,
    prev_unit_price,
    last_prev_unit_price,
    distinct_warehouse_id_in_qty_tercile_bucket,
    GREATEST(qty_decile, last_prev_unit_price) AS greatest_qty_decile_last_prev_unit_price
FROM ranked
ORDER BY warehouse_id_move_type_max_unit_price, unit_price_percent_rank DESC
