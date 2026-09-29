WITH base_moves AS (
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
        qty - unit_price AS qty_minus_unit_price,
        ABS(qty - 154) AS qty_distance
    FROM stock_moves
    WHERE operator IN ('op_bee', 'op_ann')
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type NOT IN ('ADJUST', 'TRANSFER')
              AND st.moved_at >= TIMESTAMP '2025-01-31 15:00:00'
        )
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        qty,
        moved_at,
        unit_price,
        qty_distance,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, warehouse_id ORDER BY qty, id), 3) AS qty_cume_dist,
        DENSE_RANK() OVER w_sku_id_warehouse_id_by_qty AS qty_dense_rank,
        COUNT(*) OVER (PARTITION BY warehouse_id, YEAR(moved_at)) AS rows_in_warehouse_id_moved_at_year,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, YEAR(moved_at))) AS distinct_operator_in_warehouse_id_moved_at_year
    FROM without_matches
    WINDOW w_sku_id_warehouse_id_by_qty AS (PARTITION BY sku_id, warehouse_id ORDER BY qty, id)
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(CASE WHEN move_type = 'ADJUST' THEN 1 END) AS move_move_type_count,
        MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count,
        st.last_moved_at
    FROM ranked s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
totals AS (
    -- Summarise per group
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        warehouse_id,
        distinct_operator_in_warehouse_id_moved_at_year,
        MIN(moved_at) AS first_moved_at,
        COUNT(*) AS cnt,
        ROUND(SUM(move_move_type_count) / NULLIF(SUM(qty), 0), 3) AS move_move_type_count_to_qty_ratio,
        ROUND(SUM(rows_in_warehouse_id_moved_at_year) / NULLIF(SUM(unit_price), 0), 3) AS rows_in_warehouse_id_moved_at_year_to_unit_price,
        ROUND(STDDEV(qty_distance), 2) AS stddev_qty_distance,
        MAX(moved_at) AS last_moved_at_2
    FROM with_counts
    WHERE qty BETWEEN 138 AND 387
    GROUP BY 1, 2, 3
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.sku_id <= 7
              AND st.unit_price BETWEEN 161.4 AND 328.3
        )
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on, w.city AS warehouse_city
    FROM qualified s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    moved_at_day,
    warehouse_id,
    move_move_type_count_to_qty_ratio,
    stddev_qty_distance,
    last_moved_at_2,
    warehouse_opened_on,
    warehouse_city,
    SUBSTRING(TRIM(warehouse_city), 1, 4) AS warehouse_city_prefix,
    DATEDIFF(DATE '2025-12-31', first_moved_at) AS days_since_first_moved_at
FROM with_details_warehouse
ORDER BY warehouse_city_prefix DESC
