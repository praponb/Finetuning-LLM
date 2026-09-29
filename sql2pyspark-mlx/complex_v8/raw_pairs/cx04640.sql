WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
    FROM stock_moves
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.move_type_slug,
        s.operator_prefix,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM valid s
    JOIN warehouses w ON w.id = s.warehouse_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.move_type_slug,
        s.warehouse_wh_code,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity
    FROM joined s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_city,
        warehouse_id,
        MIN(warehouse_capacity) AS min_warehouse_capacity,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        MAX(moved_at) AS last_moved_at,
        SUM(warehouse_capacity) AS total_warehouse_capacity,
        COUNT(*) AS row_count
    FROM combined_details
    GROUP BY warehouse_city, warehouse_id
)
SELECT
    warehouse_id,
    row_count,
    MIN(last_moved_at) AS first_last_moved_at,
    SUM(row_count) AS total_row_count,
    COUNT(*) AS n_rows
FROM totals
GROUP BY GROUPING SETS ((warehouse_id, row_count), (warehouse_id), (row_count))
ORDER BY warehouse_id NULLS LAST, row_count NULLS LAST
