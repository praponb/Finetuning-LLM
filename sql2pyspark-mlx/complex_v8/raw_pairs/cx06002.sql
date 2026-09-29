WITH base AS (
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
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        CASE WHEN move_type IN ('OUTBOUND', 'TRANSFER') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM stock_moves
    WHERE sku_id >= 15
),
scored AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label
    FROM base
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_slug,
        w.wh_code AS warehouse_wh_code,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM scored s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        warehouse_wh_code,
        sku_code,
        sku_unit_cost,
        UPPER(TRIM(move_type)) AS move_type_clean,
        TO_DATE(moved_at) AS moved_at_date,
        INITCAP(TRIM(move_type)) AS move_type_clean_2
    FROM combined_details_warehouse
    WHERE sku_unit_cost BETWEEN 60.9 AND 277.0
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        sku_code,
        sku_unit_cost,
        move_type_clean,
        UPPER(TRIM(warehouse_wh_code)) AS warehouse_wh_code_clean
    FROM scored_2
),
move_type_stats AS (
    -- Max sku_unit_cost per move_type
    SELECT move_type, MAX(sku_unit_cost) AS max_sku_unit_cost
    FROM tagged
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_sku_unit_cost,
        ROUND(s.sku_unit_cost - g.max_sku_unit_cost, 2) AS sku_unit_cost_minus_max
    FROM tagged s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.sku_unit_cost = g.max_sku_unit_cost
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM outliers s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
-- Subtotals with CUBE
SELECT
    sku_id,
    move_count,
    ROUND(SUM(sku_unit_cost_minus_max), 2) AS total_sku_unit_cost_minus_max,
    MIN(moved_at) AS first_moved_at,
    COUNT(*) AS n_rows
FROM profiled
GROUP BY CUBE(sku_id, move_count)
ORDER BY sku_id NULLS LAST, move_count NULLS LAST
