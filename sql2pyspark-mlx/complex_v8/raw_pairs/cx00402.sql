WITH cleaned AS (
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
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at_2
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per warehouse_id, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        days_since_moved_at,
        days_since_moved_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY days_since_moved_at DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
stats AS (
    -- Group-level totals
    SELECT
        sku_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        SUM(qty) AS total_qty,
        MAX(moved_at) AS last_moved_at,
        SUM(CASE WHEN operator IS NOT NULL THEN 1 ELSE 0 END) AS n_operator_is_not_null
    FROM unique_rows
    GROUP BY sku_id
    HAVING MAX(days_since_moved_at_2) > 210
)
-- Native SQL UNPIVOT (wide to long)
SELECT sku_id, metric, reading
FROM (
    SELECT sku_id, n_operator_is_not_null, total_qty
    FROM stats
) src
UNPIVOT (
    reading FOR metric IN (n_operator_is_not_null, total_qty)
) unp
ORDER BY sku_id, metric
