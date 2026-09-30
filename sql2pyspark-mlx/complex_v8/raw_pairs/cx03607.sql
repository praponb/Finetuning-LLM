WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        ABS(qty - 244) AS qty_distance
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MIN(unit_price) AS min_move_unit_price,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.min_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count
    FROM recent_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, w.wh_code AS warehouse_wh_code, w.capacity AS warehouse_capacity
    FROM summarized_rows s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.city = 'Bangkok'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(moved_at) AS moved_at_date,
        ROUND(min_move_unit_price / NULLIF(warehouse_capacity, 0), 3) AS min_move_unit_price_per_warehouse_capacity
    FROM combined_details
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.warehouse_id <= 5
              AND st.qty >= 145
        )
      AND s.move_count > 4
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(move_count) OVER (PARTITION BY operator, DATE_TRUNC('MONTH', moved_at_date)) AS operator_moved_at_date_month_min_move_count,
        LEAD(moved_at) OVER (PARTITION BY operator, sku_id ORDER BY move_count ASC, id) AS next_moved_at,
        COUNT(*) OVER (PARTITION BY operator, DATE_TRUNC('MONTH', moved_at_date)) AS rows_in_operator_moved_at_date_month,
        MAX(qty_distance) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_distance_max_3_prior,
        SUM(unit_price) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        LEAD(moved_at) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price, id) AS next_moved_at_2
    FROM qualified
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.operator_slug,
        s.qty_distance,
        s.move_count,
        s.warehouse_capacity,
        s.moved_at_date,
        s.operator_moved_at_date_month_min_move_count,
        s.next_moved_at,
        s.rows_in_operator_moved_at_date_month,
        s.qty_distance_max_3_prior,
        s.running_unit_price,
        s.next_moved_at_2,
        st.id AS move_id,
        st.operator AS move_operator,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price
    FROM sequenced s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    sku_id,
    operator_slug,
    qty_distance,
    move_count,
    warehouse_capacity,
    moved_at_date,
    operator_moved_at_date_month_min_move_count,
    next_moved_at,
    rows_in_operator_moved_at_date_month,
    qty_distance_max_3_prior,
    running_unit_price,
    next_moved_at_2,
    move_id,
    move_operator,
    move_warehouse_id,
    move_unit_price,
    DATE_FORMAT(next_moved_at_2, 'yyyy-MM-dd HH:00') AS next_moved_at_2_label
FROM detail_rows
ORDER BY rows_in_operator_moved_at_date_month
