WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
    FROM stock_moves
    WHERE warehouse_id >= 3
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at_dayofweek,
        s.operator_slug,
        sk.unit_cost AS sku_unit_cost,
        sk.sku_code,
        w.capacity AS warehouse_capacity
    FROM prepared_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2,
        COUNT(*) AS move_count_3
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2,
        COALESCE(st.move_count_3, 0) AS move_count_3
    FROM with_parent_sku s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.sku_unit_cost,
        s.warehouse_capacity,
        s.move_count,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.operator AS move_operator,
        st.move_type
    FROM with_stats s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.sku_unit_cost,
        s.warehouse_capacity,
        s.move_id,
        s.move_warehouse_id,
        s.move_operator,
        s.move_type,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM line_level s
    JOIN skus sk ON sk.id = s.sku_id
),
with_stats_2 AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_qty
    FROM joined_sku s
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(sku_unit_cost, warehouse_capacity) AS least_sku_unit_cost_warehouse_capacity,
        SUBSTRING(TRIM(sku_category), 1, 3) AS sku_category_prefix
    FROM with_stats_2
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, metric_name, metric_value
FROM (
    SELECT id, move_id, least_sku_unit_cost_warehouse_capacity, sku_unit_cost
    FROM enriched
) src
UNPIVOT (
    metric_value FOR metric_name IN (least_sku_unit_cost_warehouse_capacity, sku_unit_cost)
) unp
