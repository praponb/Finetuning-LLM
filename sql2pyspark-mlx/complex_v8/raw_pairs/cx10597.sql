WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR warehouse_id IS NOT NULL)
),
joined_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels
    FROM cleaned_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.capacity > 4912
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY move_type ORDER BY unit_price ASC, id) AS unit_price_decile,
        MAX(warehouse_capacity) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_max_warehouse_capacity,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY warehouse_id, operator), 2) AS warehouse_capacity_vs_warehouse_id_operator_avg
    FROM joined_warehouse
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS min_move_unit_price
    FROM windowed s
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM profiled
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    moved_at,
    unit_price,
    warehouse_capacity,
    sku_labels,
    unit_price_decile,
    warehouse_id_operator_max_warehouse_capacity,
    warehouse_capacity_vs_warehouse_id_operator_avg,
    min_move_unit_price,
    moved_at_label,
    days_since_moved_at,
    CASE WHEN min_move_unit_price >= 325.4 THEN 'A' WHEN min_move_unit_price >= 175.7 THEN 'B' ELSE 'C' END AS min_move_unit_price_tier,
    SUBSTRING(TRIM(moved_at_label), 1, 3) AS moved_at_label_prefix
FROM scored
