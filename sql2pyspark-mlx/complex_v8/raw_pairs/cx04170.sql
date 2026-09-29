WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator = 'op_eve'
      AND warehouse_id <= 5
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM prepared s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        operator,
        sku_id,
        sku_is_active,
        MAX(moved_at) AS last_moved_at,
        ROUND(AVG(qty), 2) AS avg_qty,
        COUNT(*) AS n_rows,
        ROUND(SUM(least_unit_price_qty), 2) AS total_least_unit_price_qty
    FROM with_parent
    GROUP BY operator, sku_id, sku_is_active
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        FLOOR(avg_qty / 100) * 100 AS avg_qty_bucket,
        CASE WHEN operator IN ('op_chan', 'op_ann') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM grouped
    WHERE sku_is_active = TRUE
)
-- Final projection
SELECT
    operator,
    sku_id,
    last_moved_at,
    n_rows,
    total_least_unit_price_qty,
    avg_qty_bucket,
    operator_group
FROM labeled
ORDER BY n_rows DESC, total_least_unit_price_qty DESC
