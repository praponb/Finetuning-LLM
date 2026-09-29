WITH base AS (
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
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE unit_price >= 152.7
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_bucket,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type, warehouse_id)) AS distinct_sku_id_in_move_type_warehouse_id,
        LEAD(unit_price, 3) OVER (ORDER BY qty_per_unit_price DESC, id) AS next3_unit_price,
        COUNT(*) OVER (PARTITION BY move_type, warehouse_id) AS rows_in_move_type_warehouse_id,
        qty_per_unit_price - LAG(qty_per_unit_price) OVER w_all_by_unit_price_bucket AS qty_per_unit_price_change,
        DENSE_RANK() OVER w_all_by_unit_price_bucket AS unit_price_bucket_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY unit_price_bucket ASC, id) AS rn,
        LAST_VALUE(qty_per_unit_price) IGNORE NULLS OVER (PARTITION BY operator ORDER BY unit_price_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty_per_unit_price,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator), 2) AS qty_vs_operator_avg,
        NTILE(3) OVER (ORDER BY unit_price_bucket ASC, id) AS unit_price_bucket_tercile,
        ROUND(unit_price_bucket - AVG(unit_price_bucket) OVER (PARTITION BY move_type, warehouse_id), 2) AS unit_price_bucket_vs_move_type_warehouse_id_avg
    FROM base
    WINDOW w_all_by_unit_price_bucket AS (ORDER BY unit_price_bucket ASC, id), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE rn <= 3
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price_bucket,
    distinct_sku_id_in_move_type_warehouse_id,
    rows_in_move_type_warehouse_id,
    qty_per_unit_price_change,
    rn,
    last_known_qty_per_unit_price,
    qty_vs_operator_avg,
    unit_price_bucket_tercile,
    ROUND(qty / NULLIF(qty_vs_operator_avg, 0), 3) AS qty_per_qty_vs_operator_avg,
    DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
FROM top_rows
ORDER BY qty_per_qty_vs_operator_avg DESC
