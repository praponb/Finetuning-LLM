WITH prepared_moves AS (
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
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM stock_moves
    WHERE sku_id >= 14
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        unit_price_per_qty,
        LEAD(unit_price_per_qty, 1, 0) OVER w_warehouse_id_by_unit_price AS next_unit_price_per_qty,
        ROUND(unit_price_per_qty - AVG(unit_price_per_qty) OVER w_move_type, 2) AS unit_price_per_qty_vs_move_type_avg,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at) AS moved_at_dense_rank,
        SUM(unit_price) OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        FIRST_VALUE(qty) OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_2
    FROM prepared_moves
    WINDOW w_warehouse_id_by_unit_price AS (PARTITION BY warehouse_id ORDER BY unit_price ASC, id), w_move_type AS (PARTITION BY move_type)
),
unique_rows AS (
    -- Keep the earliest row per sku_id, move_type
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        unit_price_per_qty,
        next_unit_price_per_qty,
        unit_price_per_qty_vs_move_type_avg,
        moved_at_dense_rank,
        running_unit_price,
        first_qty,
        unit_price_avg_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY running_unit_price, id) AS row_num
        FROM sequenced
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with GROUPING SETS
SELECT
    sku_id,
    moved_at_dense_rank,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(unit_price_avg_2) / NULLIF(SUM(moved_at_dense_rank), 0), 3) AS unit_price_avg_2_to_moved_at_dense_rank_ratio,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN move_type IS NOT NULL THEN next_unit_price_per_qty ELSE 0 END) AS next_unit_price_per_qty_move_type_is_not_null
FROM unique_rows
GROUP BY GROUPING SETS ((sku_id, moved_at_dense_rank), (sku_id), ())
