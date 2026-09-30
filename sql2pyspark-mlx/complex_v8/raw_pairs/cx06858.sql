WITH valid_moves AS (
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
    WHERE sku_id >= 2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.weight_kg <= 18.5
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'INBOUND') FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_move_type_count
    FROM enriched s
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        COUNT(*) FILTER (WHERE operator NOT IN ('op_chan', 'op_dew')) AS move_operator_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_operator_count, 0) AS move_operator_count
    FROM profiled s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.warehouse_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(move_operator_count / NULLIF(move_move_type_count, 0), 3) AS move_operator_count_per_move_move_type_count,
        SUBSTRING(TRIM(warehouse_wh_code), 1, 2) AS warehouse_wh_code_prefix
    FROM with_stats
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.sku_code,
        s.sku_weight_kg,
        s.warehouse_wh_code,
        s.warehouse_opened_on,
        s.move_move_type_count,
        s.move_count,
        s.move_operator_count,
        s.move_operator_count_per_move_move_type_count,
        s.warehouse_wh_code_prefix,
        w.opened_on AS warehouse_opened_on_2,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM derived s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    unit_price,
    sku_code,
    sku_weight_kg,
    warehouse_wh_code,
    move_move_type_count,
    move_count,
    move_operator_count,
    move_operator_count_per_move_move_type_count,
    warehouse_wh_code_prefix,
    warehouse_opened_on_2,
    sku_unit_cost,
    CASE WHEN move_operator_count >= 6 THEN 'HIGH' WHEN move_operator_count >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS move_operator_count_tier
FROM enriched_2
