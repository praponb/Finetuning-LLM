WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND move_type NOT IN ('OUTBOUND', 'ADJUST')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'TRANSFER') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count
    FROM with_parent s
)
-- Final projection
SELECT
    id,
    qty,
    operator,
    sku_unit_cost,
    sku_labels,
    FLOOR(qty / 100) * 100 AS qty_bucket,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM profiled
ORDER BY sku_unit_cost, qty NULLS LAST, id
LIMIT 20
