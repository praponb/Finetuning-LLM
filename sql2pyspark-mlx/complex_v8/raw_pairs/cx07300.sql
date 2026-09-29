WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM stock_moves
    WHERE (unit_price <= 323.9 OR operator IS NOT NULL)
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM prepared_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IN ('op_eve', 'op_bee', 'op_ann')
              AND MONTH(st.moved_at) IN (2, 3)
        )
),
joined AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.qty_per_unit_price,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM filtered s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at
    FROM joined s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
labeled AS (
    -- Add calculated columns
    SELECT *, COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM with_stats
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', move_type_or_unknown, move_type) AS move_type_or_unknown_move_type_label,
        last_moved_at + INTERVAL 1 DAYS AS last_moved_at_plus_1d
    FROM labeled
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS max_move_qty,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_unit_price,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at_2
    FROM derived s
)
SELECT id, metric, reading
FROM (
    SELECT id, max_move_qty, qty
    FROM profiled
) src
UNPIVOT (
    reading FOR metric IN (max_move_qty, qty)
) unp
ORDER BY id, metric
