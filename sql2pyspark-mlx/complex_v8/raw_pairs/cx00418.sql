WITH valid_moves AS (
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
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND move_type IN ('TRANSFER', 'ADJUST', 'OUTBOUND')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_min_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, move_type), 2) AS unit_price_vs_sku_id_move_type_avg,
        unit_price - LAG(unit_price) OVER (PARTITION BY operator, move_type ORDER BY moved_at, id) AS unit_price_change,
        MIN(qty) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_qty,
        COUNT(*) OVER (PARTITION BY sku_id, move_type) AS rows_in_sku_id_move_type,
        LAST_VALUE(moved_at) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id), 2) AS warehouse_id_avg_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, move_type ORDER BY qty ASC), 3) AS qty_cume_dist,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id, move_type)) AS distinct_operator_in_sku_id_move_type
    FROM valid_moves
),
labeled AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM windowed
    WHERE last_moved_at < TIMESTAMP '2025-02-12 15:00:00'
)
-- Final projection
SELECT
    id,
    qty,
    operator,
    unit_price,
    move_type_ref,
    sku_id_move_type_min_unit_price,
    unit_price_vs_sku_id_move_type_avg,
    unit_price_change,
    rows_in_sku_id_move_type,
    warehouse_id_avg_unit_price,
    qty_cume_dist,
    distinct_operator_in_sku_id_move_type,
    move_type_prefix,
    ROUND(unit_price * 0.9, 2) AS unit_price_adj,
    LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
FROM labeled
