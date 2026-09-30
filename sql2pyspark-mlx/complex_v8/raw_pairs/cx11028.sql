WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty < 348
      AND sku_id <= 12
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM valid s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_count,
        sk.labels AS sku_labels,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city
    FROM profiled s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        move_count,
        sku_labels,
        warehouse_wh_code,
        warehouse_city,
        COALESCE(move_count, 0) AS move_count_filled,
        CASE WHEN unit_price >= 307.8 THEN 'HIGH' WHEN unit_price >= 180.0 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_tier,
        LOWER(REGEXP_REPLACE(TRIM(warehouse_wh_code), ' ', '_')) AS warehouse_wh_code_slug
    FROM with_details
    WHERE moved_at >= TIMESTAMP '2025-02-06 08:00:00'
)
-- Subtotals with GROUPING SETS
SELECT
    move_count,
    sku_id,
    move_type,
    GROUPING(move_count) AS is_total_move_count,
    SUM(move_count_filled) AS total_move_count_filled
FROM enriched
GROUP BY GROUPING SETS ((move_count, sku_id, move_type), (move_count), (sku_id))
ORDER BY move_count NULLS LAST, sku_id NULLS LAST, move_type NULLS LAST
