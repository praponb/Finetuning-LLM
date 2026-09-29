WITH cleaned_moves AS (
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
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND unit_price < 277.6
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_unit_price AS unit_price_dense_rank,
        COUNT(*) OVER w_warehouse_id AS rows_in_warehouse_id,
        NTH_VALUE(qty, 2) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        FIRST_VALUE(qty_bucket) OVER (PARTITION BY sku_id ORDER BY qty_bucket NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty_bucket,
        SUM(qty) OVER w_warehouse_id AS warehouse_id_sum_qty,
        LAG(unit_price, 3) OVER (PARTITION BY operator, warehouse_id ORDER BY qty DESC, id) AS prev3_unit_price
    FROM cleaned_moves
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price DESC, id), w_warehouse_id AS (PARTITION BY warehouse_id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE unit_price_dense_rank = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        moved_at,
        operator,
        unit_price,
        qty_bucket,
        rows_in_warehouse_id,
        second_qty,
        first_qty_bucket,
        prev3_unit_price,
        NTILE(3) OVER (PARTITION BY MONTH(moved_at) ORDER BY moved_at, id) AS moved_at_tercile,
        LAST_VALUE(qty_bucket) IGNORE NULLS OVER (PARTITION BY unit_price_dense_rank, warehouse_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty_bucket,
        LAST_VALUE(prev3_unit_price) IGNORE NULLS OVER (PARTITION BY unit_price_dense_rank, warehouse_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev3_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(moved_at) ORDER BY moved_at), 3) AS moved_at_cume_dist
    FROM top_rows
)
SELECT
    id,
    warehouse_id,
    moved_at,
    operator,
    unit_price,
    qty_bucket,
    rows_in_warehouse_id,
    second_qty,
    first_qty_bucket,
    prev3_unit_price,
    last_known_prev3_unit_price,
    moved_at_cume_dist,
    ROUND(unit_price * 0.9, 2) AS unit_price_adj
FROM sequenced
