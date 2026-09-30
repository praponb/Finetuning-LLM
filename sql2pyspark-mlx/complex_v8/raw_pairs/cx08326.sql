WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 08:00:00'
      AND sku_id <= 3
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id, move_type), 2) AS qty_vs_sku_id_move_type_avg,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id, move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        MIN(qty) OVER w_sku_id_move_type AS sku_id_move_type_min_qty,
        COUNT(*) OVER (PARTITION BY sku_id, move_type) AS rows_in_sku_id_move_type
    FROM valid
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
),
latest_per_key AS (
    -- Keep the earliest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        qty_vs_sku_id_move_type_avg,
        last_known_qty,
        sku_id_move_type_min_qty,
        rows_in_sku_id_move_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY last_known_qty, id) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
SELECT
    warehouse_id,
    sku_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT operator) AS distinct_operators
FROM latest_per_key
GROUP BY GROUPING SETS ((warehouse_id, sku_id), (warehouse_id))
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
