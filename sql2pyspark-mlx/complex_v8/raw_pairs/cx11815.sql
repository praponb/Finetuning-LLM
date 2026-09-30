WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(qty, 0) AS qty_filled,
        GREATEST(qty, unit_price) AS greatest_qty_unit_price
    FROM stock_moves
    WHERE unit_price < 298.9
      AND move_type IN ('ADJUST', 'INBOUND', 'TRANSFER')
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count
    FROM valid s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.qty_filled,
        w.wh_code AS warehouse_wh_code,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM with_stats s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.wh_code IS NOT NULL
),
deduped AS (
    -- Keep the latest row per operator, sku_category
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        qty_filled,
        warehouse_wh_code,
        sku_category,
        sku_labels
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_category ORDER BY qty DESC, id DESC) AS rn
        FROM combined_details
    ) ranked
    WHERE rn = 1
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT COUNT(CASE WHEN st.operator <> 'op_dew' THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count_2
    FROM deduped s
)
-- Final projection
SELECT
    id,
    sku_id,
    operator,
    unit_price,
    qty_filled,
    sku_labels,
    move_count,
    GREATEST(unit_price, move_count) AS greatest_unit_price_move_count
FROM with_counts
ORDER BY id
LIMIT 20
